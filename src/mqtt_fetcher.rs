use crate::smart_meter_emulator::Readings;
use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions, Packet, QoS};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

/// Longest pause between two reconnect attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

pub struct MqttFetcher;

impl MqttFetcher {
    /// Connects to the broker and keeps the connection alive forever.
    ///
    /// The broker may go away at any time (e.g. Home Assistant restarting its Mosquitto
    /// add-on). rumqttc reconnects on the next `poll()`, but with a clean session the broker
    /// forgets our subscription, so we (re-)subscribe on every `ConnAck` instead of once at
    /// start-up. Reconnect attempts back off exponentially up to `MAX_BACKOFF`.
    #[allow(clippy::too_many_arguments)]
    pub async fn spawn(
        broker_host: &str,
        broker_port: u16,
        client_id: &str,
        topic: &str,
        serial: Option<String>,
        username: Option<String>,
        password: Option<String>,
        tx: Sender<Readings>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut mqttoptions = MqttOptions::new(client_id, broker_host, broker_port);
        mqttoptions.set_keep_alive(Duration::from_secs(10));
        mqttoptions.set_clean_session(true);

        if let (Some(u), Some(p)) = (username, password) {
            mqttoptions.set_credentials(u, p);
        }

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
        let topic = topic.to_string();
        let prefix_channel0 = serial.as_ref().map(|s| format!("{s}/0/"));
        println!("Connecting to MQTT broker {broker_host}:{broker_port} ...");

        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            let mut connected = false;
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        println!("MQTT connected");
                        connected = true;
                        backoff = Duration::from_secs(1);
                        // try_subscribe only queues the request; it is sent by the next poll().
                        match client.try_subscribe(topic.clone(), QoS::AtMostOnce) {
                            Ok(()) => println!("Subscribed to MQTT topic: {topic}"),
                            Err(e) => eprintln!("MQTT subscribe to {topic} failed: {e}"),
                        }
                    }
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        if let Ok(payload) = std::str::from_utf8(&publish.payload) {
                            if let Ok(val) = payload.trim().parse::<f32>() {
                                for reading in
                                    readings_for(&publish.topic, val, prefix_channel0.as_deref())
                                {
                                    let _ = tx.send(reading).await;
                                }
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        if connected {
                            eprintln!("MQTT connection lost: {}", describe(&e));
                            connected = false;
                        } else {
                            eprintln!(
                                "MQTT broker not reachable ({}), retrying in {}s",
                                describe(&e),
                                backoff.as_secs()
                            );
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                    }
                }
            }
        });

        Ok(())
    }
}

fn describe(e: &ConnectionError) -> String {
    e.to_string()
}

/// Maps an OpenDTU topic/value pair to the meter readings it updates.
fn readings_for(topic: &str, val: f32, prefix_channel0: Option<&str>) -> Vec<Readings> {
    let is_our_inverter = match prefix_channel0 {
        Some(p) => topic.contains(p),
        None => topic.contains("/0/"),
    };

    if is_our_inverter {
        if topic.ends_with("/voltage") {
            vec![
                Readings::PhaseAVoltage(val),
                Readings::AveragePhaseVoltage(val),
            ]
        } else if topic.ends_with("/current") {
            vec![Readings::PhaseACurrent(val), Readings::NetACCurrent(val)]
        } else if topic.ends_with("/power") {
            vec![Readings::PhaseAWatts(val), Readings::TotalRealPower(val)]
        } else if topic.ends_with("/frequency") {
            vec![Readings::Frequency(val)]
        } else if topic.ends_with("/powerfactor") {
            let pf = if val.abs() > 1.0 { val / 100.0 } else { val };
            vec![Readings::PhaseAPF(pf), Readings::PowerFactorTotal(pf)]
        } else if topic.ends_with("/reactivepower") {
            vec![Readings::PhaseAVAR(val), Readings::ReactivePower(val)]
        } else if topic.ends_with("/yieldtotal") {
            vec![Readings::TotalExportEnergy(val * 1000.0)]
        } else {
            vec![]
        }
    } else if topic.ends_with("ac/power") {
        vec![Readings::PhaseAWatts(val), Readings::TotalRealPower(val)]
    } else if topic.ends_with("ac/yieldtotal") {
        vec![Readings::TotalExportEnergy(val * 1000.0)]
    } else {
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_inverter_channel0_power() {
        let r = readings_for("opendtu/1234/0/power", 500.0, Some("1234/0/"));
        assert_eq!(
            r,
            vec![
                Readings::PhaseAWatts(500.0),
                Readings::TotalRealPower(500.0)
            ]
        );
    }

    #[test]
    fn ignores_other_inverters_channel() {
        assert!(readings_for("opendtu/9999/0/voltage", 230.0, Some("1234/0/")).is_empty());
    }

    #[test]
    fn maps_ac_totals_and_scales_energy() {
        assert_eq!(
            readings_for("opendtu/ac/yieldtotal", 1.5, Some("1234/0/")),
            vec![Readings::TotalExportEnergy(1500.0)]
        );
    }

    #[test]
    fn scales_powerfactor_percent() {
        assert_eq!(
            readings_for("opendtu/1234/0/powerfactor", 98.0, Some("1234/0/")),
            vec![Readings::PhaseAPF(0.98), Readings::PowerFactorTotal(0.98)]
        );
    }
}

use crate::smart_meter_emulator::Readings;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

pub struct MqttFetcher;

impl MqttFetcher {
    pub async fn spawn(
        broker_host: &str,
        broker_port: u16,
        topic: &str,
        username: Option<String>,
        password: Option<String>,
        tx: Sender<Readings>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut mqttoptions = MqttOptions::new("fronius_bridge_client", broker_host, broker_port);
        mqttoptions.set_keep_alive(Duration::from_secs(10));

        if let (Some(u), Some(p)) = (username, password) {
            mqttoptions.set_credentials(u, p);
        }

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
        client.subscribe(topic, QoS::AtMostOnce).await?;
        println!("Subscribed to MQTT topic: {topic}");

        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        if let Ok(payload_str) = std::str::from_utf8(&publish.payload) {
                            if let Ok(val) = payload_str.trim().parse::<f32>() {
                                let topic = publish.topic.as_str();

                                if topic.ends_with("/voltage") {
                                    let _ = tx.send(Readings::PhaseAVoltage(val)).await;
                                    let _ = tx.send(Readings::AveragePhaseVoltage(val)).await;
                                } else if topic.ends_with("/current") {
                                    let _ = tx.send(Readings::PhaseACurrent(val)).await;
                                    let _ = tx.send(Readings::NetACCurrent(val)).await;
                                } else if topic.ends_with("/0/power") || topic == "ac/power" {
                                    let _ = tx.send(Readings::PhaseAWatts(val)).await;
                                    let _ = tx.send(Readings::TotalRealPower(val)).await;
                                } else if topic.ends_with("/frequency") {
                                    let _ = tx.send(Readings::Frequency(val)).await;
                                } else if topic.ends_with("/powerfactor") {
                                    // Umrechnung von % in Faktor (z.B. 98.0% -> 0.98)
                                    let pf = val / 100.0;
                                    let _ = tx.send(Readings::PhaseAPF(pf)).await;
                                    let _ = tx.send(Readings::PowerFactorTotal(pf)).await;
                                } else if topic.ends_with("/reactivepower") {
                                    let _ = tx.send(Readings::PhaseAVAR(val)).await;
                                    let _ = tx.send(Readings::ReactivePower(val)).await;
                                } else if topic.ends_with("/yieldtotal") || topic == "ac/yieldtotal"
                                {
                                    // Umrechnung von kWh in Wh für SunSpec
                                    let wh = val * 1000.0;
                                    let _ = tx.send(Readings::TotalExportEnergy(wh)).await;
                                }
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("MQTT connection error: {e:?}");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });

        Ok(())
    }
}

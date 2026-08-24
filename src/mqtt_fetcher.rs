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

        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        if let Ok(payload_str) = std::str::from_utf8(&publish.payload) {
                            if let Ok(val) = payload_str.trim().parse::<f32>() {
                                let subtopic = publish.topic.as_str();

                                // Route measurements dynamically based on topic endings
                                if subtopic.ends_with("/voltage") || subtopic.ends_with("/u") {
                                    let _ = tx.send(Readings::PhaseAVoltage(val)).await;
                                    let _ = tx.send(Readings::AveragePhaseVoltage(val)).await;
                                } else if subtopic.ends_with("/current") || subtopic.ends_with("/i") {
                                    let _ = tx.send(Readings::PhaseACurrent(val)).await;
                                    let _ = tx.send(Readings::NetACCurrent(val)).await;
                                } else if subtopic.ends_with("/power") || subtopic.ends_with("/p") {
                                    let _ = tx.send(Readings::PhaseAWatts(val)).await;
                                    let _ = tx.send(Readings::TotalRealPower(val)).await;
                                } else if subtopic.ends_with("/frequency") || subtopic.ends_with("/f") {
                                    let _ = tx.send(Readings::Frequency(val)).await;
                                } else if subtopic.ends_with("/power_factor") || subtopic.ends_with("/pf") {
                                    let _ = tx.send(Readings::PhaseAPF(val)).await;
                                    let _ = tx.send(Readings::PowerFactorTotal(val)).await;
                                } else if subtopic.ends_with("/reactive_power") || subtopic.ends_with("/q") {
                                    let _ = tx.send(Readings::PhaseAVAR(val)).await;
                                    let _ = tx.send(Readings::ReactivePower(val)).await;
                                } else if subtopic.ends_with("/total_exported") || subtopic.ends_with("/export") {
                                    let _ = tx.send(Readings::TotalExportEnergy(val)).await;
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

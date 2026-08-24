use smart_meter_emulator::SmartMeterEmulator;
use std::{env, net::SocketAddr};
use tokio::net::TcpListener;
use tokio_modbus::server::tcp::{accept_tcp_connection, Server};

mod mqtt_fetcher;
mod smart_meter_emulator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let bind_str = env::var("FRONIUS_MODBUS_BIND").unwrap_or_else(|_| "0.0.0.0:1502".to_string());
    let socket_addr: SocketAddr = bind_str
        .parse()
        .expect("Invalid FRONIUS_MODBUS_BIND address");

    let slave_id: u16 = env::var("FRONIUS_MODBUS_SLAVE_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(240);

    let inverter_serial = env::var("INVERTER_SERIAL").ok();

    println!(
        "Starting Fronius Modbus bridge on: {socket_addr} (Slave ID: {slave_id}, Serial: {:?})",
        inverter_serial
    );

    // Initialisierung mit Seriennummer
    let (emulated_meter, meter_update_handle) =
        SmartMeterEmulator::new(slave_id, inverter_serial.as_deref().unwrap_or("00000001"));

    let broker_host = env::var("MQTT_BROKER_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let broker_port: u16 = env::var("MQTT_BROKER_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1883);
    let topic = env::var("MQTT_TOPIC").unwrap_or_else(|_| "opendtu/#".to_string());
    let mqtt_user = env::var("MQTT_USER").ok();
    let mqtt_password = env::var("MQTT_PASSWORD").ok();

    mqtt_fetcher::MqttFetcher::spawn(
        &broker_host,
        broker_port,
        &topic,
        inverter_serial,
        mqtt_user,
        mqtt_password,
        meter_update_handle.clone(),
    )
    .await?;

    server_context(socket_addr, emulated_meter)
        .await
        .expect("Modbus server encountered a fatal error");

    Ok(())
}

async fn server_context(
    socket_addr: SocketAddr,
    emulated_meter: SmartMeterEmulator,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(socket_addr).await?;
    let server = Server::new(listener);

    let new_service = |_socket_addr| Ok(Some(emulated_meter.clone()));
    let on_connected = |stream, socket_addr| async move {
        accept_tcp_connection(stream, socket_addr, new_service)
    };
    let on_process_error = |err| {
        eprintln!("Modbus process error: {err}");
    };

    server.serve(&on_connected, on_process_error).await?;
    Ok(())
}

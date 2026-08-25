use smart_meter_emulator::{MeterConfig, SmartMeterEmulator};
use std::{env, net::SocketAddr};
use tokio::net::TcpListener;
use tokio_modbus::server::tcp::{accept_tcp_connection, Server};

mod mqtt_fetcher;
mod smart_meter_emulator;

fn parse_bool_env(var_names: &[&str], default: bool) -> bool {
    for name in var_names {
        if let Ok(val) = env::var(name) {
            return match val.trim().to_lowercase().as_str() {
                "true" | "1" | "yes" | "on" | "y" => true,
                "false" | "0" | "no" | "off" | "n" => false,
                _ => default,
            };
        }
    }
    default
}

fn parse_u8_env(var_names: &[&str], default: u8) -> u8 {
    for name in var_names {
        if let Ok(val_str) = env::var(name) {
            if let Ok(val) = val_str.trim().parse::<u8>() {
                return val;
            }
        }
    }
    default
}

fn parse_string_env(var_names: &[&str]) -> Option<String> {
    for name in var_names {
        if let Ok(val) = env::var(name) {
            let trimmed = val.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let bind_str = env::var("FRONIUS_MODBUS_BIND")
        .or_else(|_| env::var("MODBUS_BIND"))
        .unwrap_or_else(|_| "0.0.0.0:1502".to_string());
    let socket_addr: SocketAddr = bind_str
        .parse()
        .expect("Invalid FRONIUS_MODBUS_BIND address");

    let inverter_serial = env::var("INVERTER_SERIAL").ok();

    // Meter 1 (Fronius - inverted reported power for Erzeugerzähler)
    let meter1_slave_id = parse_u8_env(&["FRONIUS_MODBUS_SLAVE_ID", "METER1_MODBUS_SLAVE_ID"], 240);
    let meter1_invert = parse_bool_env(&["FRONIUS_INVERT_POWER", "METER1_INVERT_POWER"], true);
    let meter1_serial = parse_string_env(&["FRONIUS_METER_SERIAL", "METER1_SERIAL"])
        .or_else(|| inverter_serial.clone())
        .unwrap_or_else(|| "00000001".to_string());

    // Meter 2 (EVCC - plain values for standard EVCC meter)
    let meter2_slave_id = parse_u8_env(&["EVCC_MODBUS_SLAVE_ID", "METER2_MODBUS_SLAVE_ID"], 241);
    let meter2_invert = parse_bool_env(&["EVCC_INVERT_POWER", "METER2_INVERT_POWER"], false);
    let meter2_serial =
        parse_string_env(&["EVCC_METER_SERIAL", "METER2_SERIAL"]).unwrap_or_else(|| {
            if let Some(ref sn) = inverter_serial {
                format!("{sn}_evcc")
            } else {
                "00000002".to_string()
            }
        });

    let meter_configs = vec![
        MeterConfig {
            slave_id: meter1_slave_id,
            serial_number: meter1_serial,
            invert_power: meter1_invert,
            name: "Fronius (Inverted Power)".to_string(),
        },
        MeterConfig {
            slave_id: meter2_slave_id,
            serial_number: meter2_serial,
            invert_power: meter2_invert,
            name: "EVCC (Plain Power)".to_string(),
        },
    ];

    println!("Starting Fronius / EVCC Smart Meter Emulation Modbus Bridge on: {socket_addr}");
    for cfg in &meter_configs {
        println!(
            "  -> Meter '{}': Slave ID = {}, Invert Power = {}, Serial = '{}'",
            cfg.name, cfg.slave_id, cfg.invert_power, cfg.serial_number
        );
    }

    let (emulated_meter, meter_update_handle) = SmartMeterEmulator::new(meter_configs);

    // Optional dedicated secondary listener if configured
    let meter2_bind = env::var("EVCC_MODBUS_BIND")
        .or_else(|_| env::var("METER2_MODBUS_BIND"))
        .ok();
    if let Some(m2_bind_str) = meter2_bind {
        if let Ok(m2_socket_addr) = m2_bind_str.parse::<SocketAddr>() {
            let emulator_clone = emulated_meter.clone();
            tokio::spawn(async move {
                if let Err(e) = server_context(m2_socket_addr, emulator_clone).await {
                    eprintln!("Secondary Modbus server error on {m2_socket_addr}: {e}");
                }
            });
            println!("  -> Dedicated secondary listener active on: {m2_socket_addr}");
        }
    }

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

use std::{collections::HashMap, future, pin::Pin, process, sync::Arc};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::timeout,
};
use tokio_modbus::prelude::*;

#[derive(Debug, Clone)]
pub struct MeterConfig {
    pub slave_id: u8,
    pub serial_number: String,
    pub invert_power: bool,
    pub name: String,
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct MeterInstance {
    pub slave_id: u8,
    pub serial_number: String,
    pub invert_power: bool,
    pub name: String,
    pub holding_registers: Arc<tokio::sync::Mutex<HashMap<u16, u16>>>,
}

#[derive(Clone)]
pub struct SmartMeterEmulator {
    meters: Arc<Vec<MeterInstance>>,
    default_meter: MeterInstance,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Readings {
    NetACCurrent(f32),
    PhaseACurrent(f32),
    PhaseBCurrent(f32),
    PhaseCCurrent(f32),
    AveragePhaseVoltage(f32),
    PhaseAVoltage(f32),
    PhaseBVoltage(f32),
    PhaseCVoltage(f32),
    AverageLLVoltage(f32),
    PhaseABVoltage(f32),
    PhaseBCVoltage(f32),
    PhaseCAVoltage(f32),
    Frequency(f32),
    TotalRealPower(f32),
    PhaseAWatts(f32),
    PhaseBWatts(f32),
    PhaseCWatts(f32),
    ApparentPower(f32),
    PhaseAVA(f32),
    PhaseBVA(f32),
    PhaseCVA(f32),
    ReactivePower(f32),
    PhaseAVAR(f32),
    PhaseBVAR(f32),
    PhaseCVAR(f32),
    PowerFactorTotal(f32),
    PhaseAPF(f32),
    PhaseBPF(f32),
    PhaseCPF(f32),
    TotalExportEnergy(f32),
    TotalImportEnergy(f32),
}

fn encode_sunspec_str(s: &str, register_count: usize) -> Vec<u16> {
    let mut registers = vec![0u16; register_count];
    let bytes = s.as_bytes();
    for (i, reg) in registers.iter_mut().enumerate() {
        let b1 = bytes.get(i * 2).copied().unwrap_or(0);
        let b2 = bytes.get(i * 2 + 1).copied().unwrap_or(0);
        *reg = ((b1 as u16) << 8) | (b2 as u16);
    }
    registers
}

fn set_holding_reg_f32(regs: &mut HashMap<u16, u16>, base_addr: u16, value: f32) {
    let bits: u32 = value.to_bits();
    regs.insert(base_addr, (bits >> 16) as u16);
    regs.insert(base_addr + 1, (bits & 0xFFFF) as u16);
}

impl MeterInstance {
    pub fn new(config: &MeterConfig) -> Self {
        let mut holding_registers = HashMap::new();

        // 1. SunSpec Base Marker at 40000 / 40001
        holding_registers.insert(40000, 0x5375); // 'Su'
        holding_registers.insert(40001, 0x6e53); // 'nS'

        // 2. Model 1 (Common Identification Model)
        holding_registers.insert(40002, 1); // Model ID = 1
        holding_registers.insert(40003, 65); // Model Length = 65 registers

        let mut offset = 40004;

        // Manufacturer (16 registers)
        for reg in encode_sunspec_str("Fronius", 16) {
            holding_registers.insert(offset, reg);
            offset += 1;
        }
        // Model (16 registers)
        for reg in encode_sunspec_str("Smart Meter 63A", 16) {
            holding_registers.insert(offset, reg);
            offset += 1;
        }
        // Options (8 registers)
        for reg in encode_sunspec_str("", 8) {
            holding_registers.insert(offset, reg);
            offset += 1;
        }
        // Version (8 registers)
        for reg in encode_sunspec_str("1.0", 8) {
            holding_registers.insert(offset, reg);
            offset += 1;
        }
        // Serial Number (16 registers)
        let clean_sn = if config.serial_number.trim().is_empty() {
            "00000001"
        } else {
            &config.serial_number
        };
        for reg in encode_sunspec_str(clean_sn, 16) {
            holding_registers.insert(offset, reg);
            offset += 1;
        }
        // Modbus Device Address (1 register -> Address 40068)
        holding_registers.insert(offset, config.slave_id as u16);
        offset += 1;

        // 3. Model 213 (Float AC Meter)
        holding_registers.insert(offset, 213); // Model ID = 213 (Address 40069)
        offset += 1;
        holding_registers.insert(offset, 124); // Model Length = 124 (Address 40070)
        offset += 1;

        // Initialize all 124 registers of Model 213 (40071 to 40194) to 0
        for addr in offset..(offset + 124) {
            holding_registers.insert(addr, 0);
        }
        offset += 124;

        // 4. SunSpec End-of-List Terminator (Address 40195 - 40196)
        holding_registers.insert(offset, 0xFFFF);
        holding_registers.insert(offset + 1, 0x0000);

        // Discovery probe fallbacks
        holding_registers.insert(0, 1);
        holding_registers.insert(1, 0);

        Self {
            slave_id: config.slave_id,
            serial_number: config.serial_number.clone(),
            invert_power: config.invert_power,
            name: config.name.clone(),
            holding_registers: Arc::new(tokio::sync::Mutex::new(holding_registers)),
        }
    }

    pub async fn update_reading(&self, reading: Readings) {
        let mut regs = self.holding_registers.lock().await;
        match reading {
            Readings::NetACCurrent(v) => set_holding_reg_f32(&mut regs, 40071, v),
            Readings::PhaseACurrent(v) => set_holding_reg_f32(&mut regs, 40073, v),
            Readings::PhaseBCurrent(v) => set_holding_reg_f32(&mut regs, 40075, v),
            Readings::PhaseCCurrent(v) => set_holding_reg_f32(&mut regs, 40077, v),
            Readings::AveragePhaseVoltage(v) => set_holding_reg_f32(&mut regs, 40079, v),
            Readings::PhaseAVoltage(v) => set_holding_reg_f32(&mut regs, 40081, v),
            Readings::PhaseBVoltage(v) => set_holding_reg_f32(&mut regs, 40083, v),
            Readings::PhaseCVoltage(v) => set_holding_reg_f32(&mut regs, 40085, v),
            Readings::AverageLLVoltage(v) => set_holding_reg_f32(&mut regs, 40087, v),
            Readings::PhaseABVoltage(v) => set_holding_reg_f32(&mut regs, 40089, v),
            Readings::PhaseBCVoltage(v) => set_holding_reg_f32(&mut regs, 40091, v),
            Readings::PhaseCAVoltage(v) => set_holding_reg_f32(&mut regs, 40093, v),
            Readings::Frequency(v) => set_holding_reg_f32(&mut regs, 40095, v),
            Readings::TotalRealPower(v) => {
                let val = if self.invert_power {
                    if v == 0.0 {
                        0.0
                    } else {
                        -v
                    }
                } else {
                    v
                };
                set_holding_reg_f32(&mut regs, 40097, val);
            }
            Readings::PhaseAWatts(v) => {
                let val = if self.invert_power {
                    if v == 0.0 {
                        0.0
                    } else {
                        -v
                    }
                } else {
                    v
                };
                set_holding_reg_f32(&mut regs, 40099, val);
            }
            Readings::PhaseBWatts(v) => {
                let val = if self.invert_power {
                    if v == 0.0 {
                        0.0
                    } else {
                        -v
                    }
                } else {
                    v
                };
                set_holding_reg_f32(&mut regs, 40101, val);
            }
            Readings::PhaseCWatts(v) => {
                let val = if self.invert_power {
                    if v == 0.0 {
                        0.0
                    } else {
                        -v
                    }
                } else {
                    v
                };
                set_holding_reg_f32(&mut regs, 40103, val);
            }
            Readings::ApparentPower(v) => set_holding_reg_f32(&mut regs, 40105, v),
            Readings::PhaseAVA(v) => set_holding_reg_f32(&mut regs, 40107, v),
            Readings::PhaseBVA(v) => set_holding_reg_f32(&mut regs, 40109, v),
            Readings::PhaseCVA(v) => set_holding_reg_f32(&mut regs, 40111, v),
            Readings::ReactivePower(v) => set_holding_reg_f32(&mut regs, 40113, v),
            Readings::PhaseAVAR(v) => set_holding_reg_f32(&mut regs, 40115, v),
            Readings::PhaseBVAR(v) => set_holding_reg_f32(&mut regs, 40117, v),
            Readings::PhaseCVAR(v) => set_holding_reg_f32(&mut regs, 40119, v),
            Readings::PowerFactorTotal(v) => set_holding_reg_f32(&mut regs, 40121, v),
            Readings::PhaseAPF(v) => set_holding_reg_f32(&mut regs, 40123, v),
            Readings::PhaseBPF(v) => set_holding_reg_f32(&mut regs, 40125, v),
            Readings::PhaseCPF(v) => set_holding_reg_f32(&mut regs, 40127, v),
            Readings::TotalExportEnergy(v) => set_holding_reg_f32(&mut regs, 40129, v),
            Readings::TotalImportEnergy(v) => set_holding_reg_f32(&mut regs, 40137, v),
        }
    }

    pub async fn read_registers(
        &self,
        addr: u16,
        cnt: u16,
    ) -> Result<Vec<u16>, tokio_modbus::ExceptionCode> {
        let regs = self.holding_registers.lock().await;
        register_read(&regs, addr, cnt)
    }
}

impl SmartMeterEmulator {
    pub fn new(configs: Vec<MeterConfig>) -> (Self, Sender<Readings>) {
        assert!(
            !configs.is_empty(),
            "At least one meter configuration required"
        );

        let instances: Vec<MeterInstance> = configs.iter().map(MeterInstance::new).collect();
        let default_meter = instances[0].clone();
        let meters = Arc::new(instances);

        let (tx, rx) = mpsc::channel(128);
        let update_meters = meters.clone();

        tokio::spawn(async move {
            Self::handle_incoming_register_events(rx, update_meters).await;
        });

        (
            Self {
                meters,
                default_meter,
            },
            tx,
        )
    }

    #[allow(dead_code)]
    pub fn new_single(
        slave_id: u8,
        serial_number: &str,
        invert_power: bool,
    ) -> (Self, Sender<Readings>) {
        Self::new(vec![MeterConfig {
            slave_id,
            serial_number: serial_number.to_string(),
            invert_power,
            name: "SingleMeter".to_string(),
        }])
    }

    async fn handle_incoming_register_events(
        mut events: Receiver<Readings>,
        meters: Arc<Vec<MeterInstance>>,
    ) {
        println!(
            "Started Modbus register update handler for {} meter(s)",
            meters.len()
        );
        let data_update_timeout = tokio::time::Duration::from_secs(30);

        while let Ok(Some(reading)) = timeout(data_update_timeout, events.recv()).await {
            for meter in meters.iter() {
                meter.update_reading(reading).await;
            }
        }
        eprintln!("No readings updates received in 30s, exiting");
        process::exit(1);
    }
}

impl tokio_modbus::server::Service for SmartMeterEmulator {
    type Request = SlaveRequest<'static>;
    type Response = Response;
    type Exception = tokio_modbus::ExceptionCode;
    type Future =
        Pin<Box<dyn future::Future<Output = Result<Self::Response, Self::Exception>> + Send>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let slave_id = req.slave;
        let meter = self
            .meters
            .iter()
            .find(|m| m.slave_id == slave_id)
            .cloned()
            .or_else(|| {
                // If slave_id is 0 or 255 (broadcast / non-specific Modbus TCP unit ID),
                // fall back to default meter
                if slave_id == 0 || slave_id == 255 {
                    Some(self.default_meter.clone())
                } else {
                    None
                }
            });

        let request = req.request;
        Box::pin(async move {
            let meter = meter.ok_or(tokio_modbus::ExceptionCode::GatewayTargetDevice)?;
            match request {
                Request::ReadInputRegisters(addr, cnt) => meter
                    .read_registers(addr, cnt)
                    .await
                    .map(Response::ReadInputRegisters),
                Request::ReadHoldingRegisters(addr, cnt) => meter
                    .read_registers(addr, cnt)
                    .await
                    .map(Response::ReadHoldingRegisters),
                _ => Err(tokio_modbus::ExceptionCode::IllegalFunction),
            }
        })
    }
}

fn register_read(
    registers: &HashMap<u16, u16>,
    addr: u16,
    cnt: u16,
) -> Result<Vec<u16>, tokio_modbus::ExceptionCode> {
    (0..cnt)
        .map(|i| {
            let reg_addr = addr + i;
            registers
                .get(&reg_addr)
                .copied()
                .ok_or(tokio_modbus::ExceptionCode::IllegalDataAddress)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_f32(reg1: u16, reg2: u16) -> f32 {
        let bits = ((reg1 as u32) << 16) | (reg2 as u32);
        f32::from_bits(bits)
    }

    #[test]
    fn test_encode_sunspec_str() {
        let encoded = encode_sunspec_str("Fronius", 16);
        assert_eq!(encoded.len(), 16);
        assert_eq!(encoded[0], 0x4672); // 'Fr'
        assert_eq!(encoded[1], 0x6f6e); // 'on'
        assert_eq!(encoded[2], 0x6975); // 'iu'
        assert_eq!(encoded[3], 0x7300); // 's\0'
        assert_eq!(encoded[4], 0x0000);
    }

    #[tokio::test]
    async fn test_meter_instance_initialization() {
        let config = MeterConfig {
            slave_id: 126,
            serial_number: "12345678".to_string(),
            invert_power: true,
            name: "FroniusMeter".to_string(),
        };
        let meter = MeterInstance::new(&config);

        // SunSpec marker
        let marker = meter.read_registers(40000, 2).await.unwrap();
        assert_eq!(marker, vec![0x5375, 0x6e53]);

        // Model 1 header
        let m1_header = meter.read_registers(40002, 2).await.unwrap();
        assert_eq!(m1_header, vec![1, 65]);

        // Device address at 40068
        let dev_addr = meter.read_registers(40068, 1).await.unwrap();
        assert_eq!(dev_addr, vec![126]);

        // Model 213 header at 40069
        let m213_header = meter.read_registers(40069, 2).await.unwrap();
        assert_eq!(m213_header, vec![213, 124]);

        // End of list marker at 40195
        let eol = meter.read_registers(40195, 2).await.unwrap();
        assert_eq!(eol, vec![0xFFFF, 0x0000]);

        // Fallbacks
        let fallback = meter.read_registers(0, 2).await.unwrap();
        assert_eq!(fallback, vec![1, 0]);
    }

    #[tokio::test]
    async fn test_power_inversion_vs_plain() {
        let fronius_cfg = MeterConfig {
            slave_id: 240,
            serial_number: "11111111".to_string(),
            invert_power: true,
            name: "Fronius".to_string(),
        };
        let evcc_cfg = MeterConfig {
            slave_id: 241,
            serial_number: "22222222".to_string(),
            invert_power: false,
            name: "EVCC".to_string(),
        };

        let fronius_meter = MeterInstance::new(&fronius_cfg);
        let evcc_meter = MeterInstance::new(&evcc_cfg);

        // Apply positive power reading (e.g., 600W generation)
        fronius_meter
            .update_reading(Readings::TotalRealPower(600.0))
            .await;
        fronius_meter
            .update_reading(Readings::PhaseAWatts(600.0))
            .await;
        fronius_meter
            .update_reading(Readings::PhaseAVoltage(230.0))
            .await;
        fronius_meter
            .update_reading(Readings::TotalExportEnergy(50000.0))
            .await;

        evcc_meter
            .update_reading(Readings::TotalRealPower(600.0))
            .await;
        evcc_meter
            .update_reading(Readings::PhaseAWatts(600.0))
            .await;
        evcc_meter
            .update_reading(Readings::PhaseAVoltage(230.0))
            .await;
        evcc_meter
            .update_reading(Readings::TotalExportEnergy(50000.0))
            .await;

        // Fronius meter should have -600.0 for power
        let f_power_regs = fronius_meter.read_registers(40097, 2).await.unwrap();
        let f_power = decode_f32(f_power_regs[0], f_power_regs[1]);
        assert_eq!(f_power, -600.0);

        let f_phase_a_regs = fronius_meter.read_registers(40099, 2).await.unwrap();
        let f_phase_a = decode_f32(f_phase_a_regs[0], f_phase_a_regs[1]);
        assert_eq!(f_phase_a, -600.0);

        // EVCC meter should have +600.0 for power
        let e_power_regs = evcc_meter.read_registers(40097, 2).await.unwrap();
        let e_power = decode_f32(e_power_regs[0], e_power_regs[1]);
        assert_eq!(e_power, 600.0);

        let e_phase_a_regs = evcc_meter.read_registers(40099, 2).await.unwrap();
        let e_phase_a = decode_f32(e_phase_a_regs[0], e_phase_a_regs[1]);
        assert_eq!(e_phase_a, 600.0);

        // Voltage and energy should NOT be inverted on either meter
        let f_voltage_regs = fronius_meter.read_registers(40081, 2).await.unwrap();
        let f_voltage = decode_f32(f_voltage_regs[0], f_voltage_regs[1]);
        assert_eq!(f_voltage, 230.0);

        let e_voltage_regs = evcc_meter.read_registers(40081, 2).await.unwrap();
        let e_voltage = decode_f32(e_voltage_regs[0], e_voltage_regs[1]);
        assert_eq!(e_voltage, 230.0);

        let f_energy_regs = fronius_meter.read_registers(40129, 2).await.unwrap();
        let f_energy = decode_f32(f_energy_regs[0], f_energy_regs[1]);
        assert_eq!(f_energy, 50000.0);

        let e_energy_regs = evcc_meter.read_registers(40129, 2).await.unwrap();
        let e_energy = decode_f32(e_energy_regs[0], e_energy_regs[1]);
        assert_eq!(e_energy, 50000.0);

        // 0.0 power check
        fronius_meter
            .update_reading(Readings::TotalRealPower(0.0))
            .await;
        let f_zero_regs = fronius_meter.read_registers(40097, 2).await.unwrap();
        let f_zero = decode_f32(f_zero_regs[0], f_zero_regs[1]);
        assert_eq!(f_zero, 0.0);
        assert_eq!(f_zero.to_bits(), 0.0f32.to_bits()); // not negative zero
    }

    #[tokio::test]
    async fn test_service_routing_by_slave_id() {
        use tokio_modbus::server::Service;

        let configs = vec![
            MeterConfig {
                slave_id: 240,
                serial_number: "FRONIUS01".to_string(),
                invert_power: true,
                name: "Fronius".to_string(),
            },
            MeterConfig {
                slave_id: 241,
                serial_number: "EVCC01".to_string(),
                invert_power: false,
                name: "EVCC".to_string(),
            },
        ];

        let (emulator, tx) = SmartMeterEmulator::new(configs);

        // Send power update
        tx.send(Readings::TotalRealPower(750.0)).await.unwrap();
        // Give background handler a tick to process
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Query Slave 240 (Fronius - inverted)
        let req_fronius = SlaveRequest {
            slave: 240,
            request: Request::ReadHoldingRegisters(40097, 2),
        };
        let res_fronius = emulator.call(req_fronius).await.unwrap();
        if let Response::ReadHoldingRegisters(regs) = res_fronius {
            let power = decode_f32(regs[0], regs[1]);
            assert_eq!(power, -750.0);
        } else {
            panic!("Unexpected response type");
        }

        // Query Slave 241 (EVCC - plain)
        let req_evcc = SlaveRequest {
            slave: 241,
            request: Request::ReadHoldingRegisters(40097, 2),
        };
        let res_evcc = emulator.call(req_evcc).await.unwrap();
        if let Response::ReadHoldingRegisters(regs) = res_evcc {
            let power = decode_f32(regs[0], regs[1]);
            assert_eq!(power, 750.0);
        } else {
            panic!("Unexpected response type");
        }

        // Query Slave 0 (fallback to default meter: Fronius)
        let req_broadcast = SlaveRequest {
            slave: 0,
            request: Request::ReadHoldingRegisters(40097, 2),
        };
        let res_broadcast = emulator.call(req_broadcast).await.unwrap();
        if let Response::ReadHoldingRegisters(regs) = res_broadcast {
            let power = decode_f32(regs[0], regs[1]);
            assert_eq!(power, -750.0);
        } else {
            panic!("Unexpected response type");
        }

        // Query unknown Slave 99 (should return GatewayTargetDeviceFailedToRespond)
        let req_unknown = SlaveRequest {
            slave: 99,
            request: Request::ReadHoldingRegisters(40097, 2),
        };
        let err_unknown = emulator.call(req_unknown).await.unwrap_err();
        assert_eq!(
            err_unknown,
            tokio_modbus::ExceptionCode::GatewayTargetDevice
        );

        // Query invalid register address
        let req_invalid_addr = SlaveRequest {
            slave: 240,
            request: Request::ReadHoldingRegisters(50000, 2),
        };
        let err_invalid = emulator.call(req_invalid_addr).await.unwrap_err();
        assert_eq!(err_invalid, tokio_modbus::ExceptionCode::IllegalDataAddress);
    }

    #[tokio::test]
    async fn test_tcp_server_multi_slave_query() {
        use tokio::net::TcpListener;
        use tokio_modbus::client::tcp;
        use tokio_modbus::client::Reader;
        use tokio_modbus::server::tcp::{accept_tcp_connection, Server};

        let configs = vec![
            MeterConfig {
                slave_id: 240,
                serial_number: "FRONIUS01".to_string(),
                invert_power: true,
                name: "Fronius".to_string(),
            },
            MeterConfig {
                slave_id: 241,
                serial_number: "EVCC01".to_string(),
                invert_power: false,
                name: "EVCC".to_string(),
            },
        ];

        let (emulator, tx) = SmartMeterEmulator::new(configs);
        tx.send(Readings::TotalRealPower(1250.0)).await.unwrap();
        tx.send(Readings::PhaseAVoltage(230.0)).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let server = Server::new(listener);

        let service = emulator.clone();
        tokio::spawn(async move {
            let new_service = |_socket_addr| Ok(Some(service.clone()));
            let on_connected = |stream, socket_addr| async move {
                accept_tcp_connection(stream, socket_addr, new_service)
            };
            let on_process_error = |err| {
                eprintln!("Test server error: {err}");
            };
            let _ = server.serve(&on_connected, on_process_error).await;
        });

        // Connect client to Slave 240 (Fronius - inverted)
        let mut client_fronius = tcp::connect_slave(local_addr, Slave(240)).await.unwrap();
        let f_regs = client_fronius
            .read_holding_registers(40097, 2)
            .await
            .unwrap()
            .unwrap();
        let f_power = decode_f32(f_regs[0], f_regs[1]);
        assert_eq!(f_power, -1250.0);

        // Connect client to Slave 241 (EVCC - plain)
        let mut client_evcc = tcp::connect_slave(local_addr, Slave(241)).await.unwrap();
        let e_regs = client_evcc
            .read_holding_registers(40097, 2)
            .await
            .unwrap()
            .unwrap();
        let e_power = decode_f32(e_regs[0], e_regs[1]);
        assert_eq!(e_power, 1250.0);

        // Verify voltage on both
        let f_v_regs = client_fronius
            .read_holding_registers(40081, 2)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decode_f32(f_v_regs[0], f_v_regs[1]), 230.0);

        let e_v_regs = client_evcc
            .read_holding_registers(40081, 2)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decode_f32(e_v_regs[0], e_v_regs[1]), 230.0);
    }
}

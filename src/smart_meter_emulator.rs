use std::{collections::HashMap, future, pin::Pin, process, sync::Arc};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::timeout,
};
use tokio_modbus::prelude::*;

#[derive(Clone)]
pub struct SmartMeterEmulator {
    holding_registers: Arc<tokio::sync::Mutex<HashMap<u16, u16>>>,
}

#[allow(dead_code)]
#[derive(Debug)]
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

impl SmartMeterEmulator {
    pub fn new(slave_id: u16, serial_number: &str) -> (Self, Sender<Readings>) {
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
        let clean_sn = if serial_number.trim().is_empty() {
            "00000001"
        } else {
            serial_number
        };
        for reg in encode_sunspec_str(clean_sn, 16) {
            holding_registers.insert(offset, reg);
            offset += 1;
        }
        // Modbus Device Address (1 register -> Address 40068)
        holding_registers.insert(offset, slave_id);
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

        let (tx, rx) = mpsc::channel(128);
        let holding_registers = Arc::new(tokio::sync::Mutex::new(holding_registers));
        let handler_holding_registers = holding_registers.clone();

        tokio::spawn(async move {
            Self::handle_incoming_register_events(rx, handler_holding_registers).await;
        });

        (Self { holding_registers }, tx)
    }

    async fn handle_incoming_register_events(
        mut events: Receiver<Readings>,
        holding_registers: Arc<tokio::sync::Mutex<HashMap<u16, u16>>>,
    ) {
        println!("Started Modbus register update handler");
        let data_update_timeout = tokio::time::Duration::from_secs(30);

        while let Ok(Some(reading)) = timeout(data_update_timeout, events.recv()).await {
            match reading {
                Readings::NetACCurrent(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40071, v).await
                }
                Readings::PhaseACurrent(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40073, v).await
                }
                Readings::PhaseBCurrent(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40075, v).await
                }
                Readings::PhaseCCurrent(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40077, v).await
                }
                Readings::AveragePhaseVoltage(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40079, v).await
                }
                Readings::PhaseAVoltage(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40081, v).await
                }
                Readings::PhaseBVoltage(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40083, v).await
                }
                Readings::PhaseCVoltage(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40085, v).await
                }
                Readings::AverageLLVoltage(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40087, v).await
                }
                Readings::PhaseABVoltage(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40089, v).await
                }
                Readings::PhaseBCVoltage(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40091, v).await
                }
                Readings::PhaseCAVoltage(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40093, v).await
                }
                Readings::Frequency(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40095, v).await
                }
                Readings::TotalRealPower(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40097, v).await
                }
                Readings::PhaseAWatts(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40099, v).await
                }
                Readings::PhaseBWatts(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40101, v).await
                }
                Readings::PhaseCWatts(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40103, v).await
                }
                Readings::ApparentPower(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40105, v).await
                }
                Readings::PhaseAVA(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40107, v).await
                }
                Readings::PhaseBVA(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40109, v).await
                }
                Readings::PhaseCVA(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40111, v).await
                }
                Readings::ReactivePower(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40113, v).await
                }
                Readings::PhaseAVAR(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40115, v).await
                }
                Readings::PhaseBVAR(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40117, v).await
                }
                Readings::PhaseCVAR(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40119, v).await
                }
                Readings::PowerFactorTotal(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40121, v).await
                }
                Readings::PhaseAPF(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40123, v).await
                }
                Readings::PhaseBPF(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40125, v).await
                }
                Readings::PhaseCPF(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40127, v).await
                }
                Readings::TotalExportEnergy(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40129, v).await
                }
                Readings::TotalImportEnergy(v) => {
                    Self::set_holding_reg_f32(&holding_registers, 40137, v).await
                }
            }
        }
        eprintln!("No readings updates received in 30s, exiting");
        process::exit(1);
    }

    async fn set_holding_reg(
        holding_registers: &Arc<tokio::sync::Mutex<HashMap<u16, u16>>>,
        register: u16,
        value: u16,
    ) {
        let mut regs = holding_registers.lock().await;
        regs.insert(register, value);
    }

    async fn set_holding_reg_f32(
        holding_registers: &Arc<tokio::sync::Mutex<HashMap<u16, u16>>>,
        register_base_number: u16,
        value: f32,
    ) {
        let bits: u32 = value.to_bits();
        Self::set_holding_reg(holding_registers, register_base_number, (bits >> 16) as u16).await;
        Self::set_holding_reg(
            holding_registers,
            register_base_number + 1,
            (bits & 0xFFFF) as u16,
        )
        .await;
    }
}

impl tokio_modbus::server::Service for SmartMeterEmulator {
    type Request = Request<'static>;
    type Response = Response;
    type Exception = tokio_modbus::ExceptionCode;
    type Future =
        Pin<Box<dyn future::Future<Output = Result<Self::Response, Self::Exception>> + Send>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let holding_registers = self.holding_registers.clone();
        Box::pin(async move {
            match req {
                Request::ReadInputRegisters(addr, cnt) => {
                    let registers = holding_registers.lock().await;
                    register_read(&registers, addr, cnt).map(Response::ReadInputRegisters)
                }
                Request::ReadHoldingRegisters(addr, cnt) => {
                    let registers = holding_registers.lock().await;
                    register_read(&registers, addr, cnt).map(Response::ReadHoldingRegisters)
                }
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

# Fronius SunSpec Smart Meter Emulation (MQTT / OpenDTU)

This service bridges MQTT power metrics (specifically tailored for **OpenDTU / Hoymiles microinverters** or generic MQTT power meters) to a **SunSpec-compliant Modbus TCP Smart Meter**. 

It enables Fronius inverters (such as the Symo, Primo, and Gen24 series) to read external AC generation data as an **auxiliary generator meter (*Erzeugerzähler*)** or sub-meter and integrate it seamlessly into the Fronius Energy Flow and Solar.web.

---

## Features

* **SunSpec Compliant:** Implements **Model 1** (Common Device Identification with custom Serial Number) and **Model 213** (3-Phase AC Float Meter layout starting at register `40000`).
* **Native OpenDTU / Hoymiles Ingestion:** Automatically consumes OpenDTU MQTT topics (`voltage`, `current`, `power`, `frequency`, `powerfactor`, `reactivepower`, `yieldtotal`) for Channel 0 (AC).
* **Automatic Unit & Sign Conversions:**
  * Inverts active power signs to match the Fronius Modbus convention for generator meters (negative register value = positive PV generation in Solar.web).
  * Automatically handles unit scaling (e.g., converts kWh to Wh and normalizes power factor percentages).
* **Watchdog Protection:** Terminates cleanly if no MQTT updates arrive within 30 seconds to prevent serving stale data to the inverter.
* **Lightweight & Async:** Built with Rust, `tokio`, `tokio-modbus`, and `rumqttc`.

---

## Architecture & Data Flow

```text
[OpenDTU / Inverter]
        │
        │ (MQTT publish: power, voltage, current, yield)
        ▼
  [MQTT Broker]
        │
        │ (rumqttc client)
        ▼
[Fronius Meter Emulation]
  ├── MQTT Fetcher Task ──(mpsc channel)──► Register Handler
  └── Modbus TCP Server (Port 1502 / 502) ◄── Holding Registers (SunSpec M1 + M213)
        ▲
        │ (Modbus TCP polling every 1-2s)
[Fronius Symo / Gen24 Inverter]
```

---

## Configuration

All configuration is provided through environment variables.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `FRONIUS_MODBUS_BIND` | `0.0.0.0:1502` | Socket address for the Modbus TCP server. |
| `FRONIUS_MODBUS_SLAVE_ID` | `240` | Modbus Unit ID / Slave ID (must match inverter config). |
| `INVERTER_SERIAL` | `00000001` | Serial number of the Hoymiles/OpenDTU inverter (used for topic filtering and SunSpec Model 1 SN). |
| `MQTT_BROKER_HOST` | `127.0.0.1` | IP address or hostname of the MQTT broker. |
| `MQTT_BROKER_PORT` | `1883` | Port of the MQTT broker. |
| `MQTT_TOPIC` | `opendtu/#` | Base MQTT topic to subscribe to (use `#` wildcard). |
| `MQTT_USER` | *(None)* | Optional username for MQTT authentication. |
| `MQTT_PASSWORD` | *(None)* | Optional password for MQTT authentication. |

---

## Docker Compose Example

```yaml
services:
  fronius-meter-emulation:
    restart: always
    network_mode: host
    image: ghcr.io/soylentorange/fronius_meter_emulation:latest
    environment:
      # Modbus settings
      # Set port to 502 or 1502 for Fronius Symo Gen24
      # (502 is problematic though, probably need to run as root)
      - FRONIUS_MODBUS_BIND=0.0.0.0:1502
      # set slave-id to 1 to 14 or 84 to 127 for Fronius Symo Gen24
      - FRONIUS_MODBUS_SLAVE_ID=126
      - INVERTER_SERIAL=119182145457

      # MQTT Broker settings
      - MQTT_BROKER_HOST=192.168.178.244
      - MQTT_BROKER_PORT=1883
      - MQTT_TOPIC=opendtu/#
      - MQTT_USER=your_user_name_for_emulator
      - MQTT_PASSWORD=your_secure_password
    healthcheck:
      test: ["CMD-SHELL", "netstat -an | grep 1502 > /dev/null || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 3
      start_period: 20s
```

---

## Fronius Inverter Setup

1. Open the Fronius web interface and log in with **Technician / Service** credentials.
2. Navigate to **Device Configuration** → **Components** → **Meters**.
3. Click **Add Meter** and configure:
   * **Meter Type:** Modbus TCP (SunSpec)
   * **IP Address:** IP of your Docker host
   * **Port:** `1502`
   * **Modbus Address (Slave ID):** `126` (matches `FRONIUS_MODBUS_SLAVE_ID`)
   * **Category / Location:** **Producer / Generator Meter** (*Erzeugerzähler* / *Weiterer Erzeuger*)
4. Save the configuration. The inverter will detect the SunSpec Model 1 & 213 layout and include the external microinverter generation in total production figures.

---

## Kudos & References

* [Photovoltaikforum: Gen24 Smart Meter Modbus TCP Emulation mit ESP32](https://www.photovoltaikforum.com/thread/224214-gen24-smart-meter-modbus-tcp-emulation-mit-esp32/)
* [OpenDTU Project](https://github.com/tbnobody/OpenDTU)
* [Ralim/fronius_meter_emulation for the initial idea](https://github.com/Ralim/fronius_meter_emulation)

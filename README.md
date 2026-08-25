# Fronius & EVCC SunSpec Smart Meter Emulation (MQTT / OpenDTU)

This service bridges MQTT power metrics (specifically tailored for **OpenDTU / Hoymiles microinverters** or generic MQTT power meters) to **SunSpec-compliant Modbus TCP Smart Meters**.

It supports **two concurrent emulated meters** with independent Slave IDs and sign conventions:
1. **Fronius Meter (Inverted Power):** Reports negative active power values (`-W`) as required by Fronius inverters (e.g. Symo, Primo, Gen24) for auxiliary generator meters (*Erzeugerzähler* / *Weiterer Erzeuger*) so external generation is integrated properly into Solar.web and Energy Flow.
2. **EVCC Meter (Plain Power):** Reports plain positive active power values (`+W`) as expected by **EVCC** and generic PV meter integrations.

---

## Features

* **Dual Emulated Meters:** Serves two Modbus Unit IDs simultaneously on the same Modbus TCP port (or optional separate ports):
  * **Meter 1 (Fronius):** Inverted active power signs for Fronius generator meter convention.
  * **Meter 2 (EVCC):** Plain / non-inverted active power signs for EVCC.
* **SunSpec Compliant:** Implements **Model 1** (Common Device Identification with custom Serial Number) and **Model 213** (3-Phase AC Float Meter layout starting at register `40000`).
* **Native OpenDTU / Hoymiles Ingestion:** Automatically consumes OpenDTU MQTT topics (`voltage`, `current`, `power`, `frequency`, `powerfactor`, `reactivepower`, `yieldtotal`) for Channel 0 (AC).
* **Automatic Unit & Sign Conversions:**
  * Handles unit scaling (converts kWh to Wh, normalizes power factor percentages).
  * Independent power sign configuration per meter.
* **Watchdog Protection:** Terminates cleanly if no MQTT updates arrive within 30 seconds to prevent serving stale data.
* **Lightweight & Async:** Built with Rust, `tokio`, `tokio-modbus`, and `rumqttc`.

---

## Architecture & Data Flow

```text
       [OpenDTU / Microinverter]
                   │
                   │ (MQTT publish: power, voltage, current, yield)
                   ▼
             [MQTT Broker]
                   │
                   │ (rumqttc client)
                   ▼
     [Fronius / EVCC Meter Emulation]
                   │
         ┌─────────┴─────────┐
         ▼                   ▼
  [Meter 1 (Fronius)]  [Meter 2 (EVCC)]
   Slave ID: 240/126    Slave ID: 241/1
   Inverted Power (-W)  Plain Power (+W)
         │                   │
         │ (Modbus TCP)      │ (Modbus TCP)
         ▼                   ▼
  [Fronius Inverter]      [EVCC]
 (Symo / Gen24 Meter)  (evcc.yaml meter)
```

---

## Configuration

All configuration is provided through environment variables.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `FRONIUS_MODBUS_BIND` | `0.0.0.0:1502` | Socket address for the primary Modbus TCP server (serves both meters). |
| `FRONIUS_MODBUS_SLAVE_ID` | `240` | Modbus Unit ID for Meter 1 (Fronius). Must match Fronius inverter config. |
| `FRONIUS_INVERT_POWER` | `true` | Invert active power signs for Meter 1 (negative = PV generation in Fronius). |
| `FRONIUS_METER_SERIAL` | `INVERTER_SERIAL` or `00000001` | Serial number for Meter 1 SunSpec Model 1 identification. |
| `EVCC_MODBUS_SLAVE_ID` | `241` | Modbus Unit ID for Meter 2 (EVCC). Set in `evcc.yaml`. |
| `EVCC_INVERT_POWER` | `false` | Invert active power signs for Meter 2 (`false` = plain positive power for EVCC). |
| `EVCC_METER_SERIAL` | `INVERTER_SERIAL` or `00000002` | Serial number for Meter 2 SunSpec Model 1 identification. |
| `EVCC_MODBUS_BIND` | *(None)* | Optional dedicated socket address for Meter 2 (if separate port is desired). |
| `INVERTER_SERIAL` | `00000001` | Base serial number of the inverter (used for topic filtering and fallback serial). |
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
      # Modbus Server Settings
      - FRONIUS_MODBUS_BIND=0.0.0.0:1502

      # Meter 1: Fronius Inverter (Inverted power for Erzeugerzähler)
      # For Gen24, choose a slave ID in range 1-14 or 84-127 (e.g. 126)
      - FRONIUS_MODBUS_SLAVE_ID=126
      - FRONIUS_INVERT_POWER=true

      # Meter 2: EVCC (Plain power values)
      - EVCC_MODBUS_SLAVE_ID=241
      - EVCC_INVERT_POWER=false

      # Inverter & Serial Numbers
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

## EVCC Setup (`evcc.yaml`)

Add the emulated meter as a PV meter or custom meter in your `evcc.yaml`:

```yaml
meters:
  - name: opendtu_pv
    type: template
    template: sunspec-inverter # or custom modbus meter
    host: 192.168.178.xxx # IP of your Docker host
    port: 1502
    id: 241 # matches EVCC_MODBUS_SLAVE_ID
```

Or using custom Modbus TCP configuration:

```yaml
meters:
  - name: opendtu_pv
    type: custom
    power:
      source: modbus
      uri: 192.168.178.xxx:1502
      id: 241
      register:
        address: 40097
        type: holding
        decode: float32
    energy:
      source: modbus
      uri: 192.168.178.xxx:1502
      id: 241
      register:
        address: 40129
        type: holding
        decode: float32
```

---

## Kudos & References

* [Photovoltaikforum: Gen24 Smart Meter Modbus TCP Emulation mit ESP32](https://www.photovoltaikforum.com/thread/224214-gen24-smart-meter-modbus-tcp-emulation-mit-esp32/)
* [OpenDTU Project](https://github.com/tbnobody/OpenDTU)
* [EVCC Project](https://evcc.io)
* [Ralim/fronius_meter_emulation for the initial idea](https://github.com/Ralim/fronius_meter_emulation)

#!/usr/bin/env python3
"""End-to-end test: the bridge must survive an MQTT broker restart.

Simulates what happens when Home Assistant restarts its Mosquitto add-on:
  1. broker up, values arrive              -> Modbus reports them
  2. broker stopped for longer than the stale timeout
                                           -> process keeps running, power reads 0 W
  3. broker started again                  -> bridge reconnects, re-subscribes, values flow

Needs Docker (for eclipse-mosquitto) and a release build of the bridge.
"""
import os
import socket
import struct
import subprocess
import sys
import time

BIN = os.environ.get("BRIDGE_BIN", "target/release/fronius_meter_emulation")
MODBUS = ("127.0.0.1", 15020)
SERIAL = "1234"
BROKER = "mq-restart-test"


def sh(*cmd, check=True):
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def wait_port(host, port, timeout=30):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection((host, port), 1):
                return
        except OSError:
            time.sleep(0.3)
    raise SystemExit(f"port {host}:{port} not reachable")


def publish(topic, value):
    sh("docker", "exec", BROKER, "mosquitto_pub", "-t", topic, "-m", str(value))


def read_f32(unit, addr):
    """Modbus TCP 'read holding registers' for two registers, decoded as big-endian f32."""
    req = struct.pack(">HHHBBHH", 1, 0, 6, unit, 3, addr, 2)
    with socket.create_connection(MODBUS, 3) as s:
        s.sendall(req)
        resp = s.recv(256)
    if resp[7] != 3:
        raise AssertionError(f"Modbus exception: {resp.hex()}")
    return struct.unpack(">f", resp[9:13])[0]


def expect(unit, addr, value, timeout, publish_topic=None):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        if publish_topic:
            publish(publish_topic, value)
        try:
            last = read_f32(unit, addr)
            if abs(last - value) < 0.01:
                return
        except OSError as e:
            last = e
        time.sleep(1)
    raise AssertionError(f"unit {unit} reg {addr}: expected {value}, got {last}")


def main():
    sh("docker", "rm", "-f", BROKER, check=False)
    sh("docker", "run", "-d", "--name", BROKER, "-p", "1883:1883",
       "eclipse-mosquitto:2", "mosquitto", "-c", "/mosquitto-no-auth.conf")
    wait_port("127.0.0.1", 1883)

    env = dict(os.environ, FRONIUS_MODBUS_BIND=f"{MODBUS[0]}:{MODBUS[1]}",
               FRONIUS_MODBUS_SLAVE_ID="126", EVCC_MODBUS_SLAVE_ID="241",
               INVERTER_SERIAL=SERIAL, MQTT_BROKER_HOST="127.0.0.1",
               MQTT_TOPIC="opendtu/#", STALE_TIMEOUT_S="5")
    log = open("bridge.log", "w")
    bridge = subprocess.Popen([BIN], env=env, stdout=log, stderr=subprocess.STDOUT)
    ok = False
    try:
        wait_port(*MODBUS)
        power = f"opendtu/{SERIAL}/0/power"

        print("1) values flow")
        publish(f"opendtu/{SERIAL}/0/voltage", 230)
        expect(241, 40097, 500.0, 15, power)
        expect(126, 40097, -500.0, 5)          # inverted meter for the Fronius

        print("2) broker down longer than stale timeout")
        sh("docker", "stop", "-t", "1", BROKER)
        time.sleep(9)
        assert bridge.poll() is None, "bridge exited while broker was down"
        expect(241, 40097, 0.0, 5)             # stale -> 0 W
        assert abs(read_f32(241, 40081) - 230.0) < 0.01, "voltage should be kept"

        print("3) broker back, bridge reconnects and re-subscribes")
        sh("docker", "start", BROKER)
        wait_port("127.0.0.1", 1883)
        expect(241, 40097, 700.0, 60, power)
        assert bridge.poll() is None, "bridge exited after reconnect"
        ok = True
        print("OK: bridge survived the broker restart")
    finally:
        bridge.terminate()
        bridge.wait(5)
        log.close()
        print("---- bridge log ----")
        print(open("bridge.log").read())
        sh("docker", "rm", "-f", BROKER, check=False)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

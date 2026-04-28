"""
Honeywell Versatilis Transmitter — Live Dashboard
Protocol spec v1.0 (28-Jan-2026)

Verified byte offsets from real sensor capture:
  byte  0 = record version (0x01)
  bytes 1-8 = spares (8 zero bytes)
  bytes 9-12 = timestamp (Int32 LE)
  byte 13 = ambient temp (signed byte, °C)
  byte 14 = humidity (%RH)
  byte 15 = pressure raw → hPa = (raw * 3) + 335
  byte 16 = surface temp (signed byte, °C)
  bytes 17-18 = RPM (uint16 LE)
  bytes 19-87 = vibration X/Y/Z (23 bytes each)
  bytes 88-94 = audio (7 bytes)
  byte 95 = battery %
  bytes 96-185 = alarm status (90 bytes) — history only
  Total history record = 186 bytes

Commands:
  2.3.1  0x01  Read Live Data
  2.3.2  0x02  Read History Data
  2.3.3  0x1B  Read Vibration RAW Data
  2.3.4  0x15  BLE Connection Heartbeat
  2.3.5  0x16  Abort + Disconnect
"""
import random
import time
import asyncio
import struct
import threading
import signal
import sys
from collections import deque
from datetime import datetime
from bleak import BleakClient
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go

# ── Config ────────────────────────────────────────────────
ADDRESS   = "00:40:84:65:20:2B"
CHAR_UUID = "e093f3b5-00a3-a9e5-9eca-40026e0edc24"
MAX_PTS   = 60

# ── Thread safety ─────────────────────────────────────────
_lock = threading.Lock()

# ── Live data store ───────────────────────────────────────
live = {
    "time":        deque(maxlen=MAX_PTS),
    "amb_temp":    deque(maxlen=MAX_PTS),
    "surf_temp":   deque(maxlen=MAX_PTS),
    "humidity":    deque(maxlen=MAX_PTS),
    "pressure":    deque(maxlen=MAX_PTS),
    "vib_x_vel":   deque(maxlen=MAX_PTS),
    "vib_y_vel":   deque(maxlen=MAX_PTS),
    "vib_z_vel":   deque(maxlen=MAX_PTS),
    "vib_x_acc":   deque(maxlen=MAX_PTS),
    "vib_y_acc":   deque(maxlen=MAX_PTS),
    "vib_z_acc":   deque(maxlen=MAX_PTS),
    "acoustics":   deque(maxlen=MAX_PTS),
    "battery":     deque(maxlen=MAX_PTS),
    "rpm":         deque(maxlen=MAX_PTS),
    "kurt_x":  0.0, "kurt_y":  0.0, "kurt_z":  0.0,
    "crest_x": 0.0, "crest_y": 0.0, "crest_z": 0.0,
    "skew_x":  0.0, "skew_y":  0.0, "skew_z":  0.0,
    "vib_x_freq": 0.0, "vib_y_freq": 0.0, "vib_z_freq": 0.0,
    "audio_freq":  0.0,
    "history":     [],
    "raw_x":       [],
    "raw_y":       [],
    "raw_z":       [],
    "ble_status":  "Disconnected",
    "last_update": "—",
    "last_cmd":    "—",
}

_ble_control = {
    "connect_requested":    True,   # auto-connect on start
    "disconnect_requested": False,
    "running":              False,
}

_packets      = []
_abort_result = {"val": None}


# ═══════════════════════════════════════════════════════════
# NOTIFICATION HANDLER
# ═══════════════════════════════════════════════════════════

def _on_notify(sender, data: bytearray):
    _packets.append(bytes(data))
    if len(data) == 1 and data[0] == 0x16:
        _abort_result["val"] = True
    if len(data) == 4 and data == b'\xDE\xAD\xFE\xED':
        _abort_result["val"] = False


# ═══════════════════════════════════════════════════════════
# 2.3.4 — HEARTBEAT
# ═══════════════════════════════════════════════════════════

async def cmd_heartbeat(client):
    try:
        await client.write_gatt_char(CHAR_UUID, bytearray([0x15]), response=False)
        print("[0x15] Heartbeat sent")
    except Exception as e:
        print(f"[0x15] Error: {e}")


# ═══════════════════════════════════════════════════════════
# 2.3.5 — ABORT
# ═══════════════════════════════════════════════════════════

async def cmd_abort(client):
    try:
        _abort_result["val"] = None
        await client.write_gatt_char(CHAR_UUID, bytearray([0x16]), response=False)
        for _ in range(20):
            await asyncio.sleep(0.1)
            if _abort_result["val"] is not None:
                break
        if _abort_result["val"] is True:
            print("[0x16] Abort accepted by sensor")
        elif _abort_result["val"] is False:
            print("[0x16] Abort rejected (0xDEADFEED)")
        else:
            print("[0x16] No abort response — proceeding anyway")
    except Exception as e:
        print(f"[0x16] Error: {e}")


async def cmd_abort_and_disconnect(client):
    print("[DISCONNECT] Sending 0x16 abort...")
    await cmd_abort(client)
    try:
        await client.stop_notify(CHAR_UUID)
    except Exception:
        pass
    with _lock:
        live["ble_status"] = "Disconnected"
        live["last_cmd"]   = "0x16 Disconnected"
    print("[DISCONNECT] Done")


# ═══════════════════════════════════════════════════════════
# PACKET COLLECTOR
# ═══════════════════════════════════════════════════════════

async def _collect(client, wait_sec: int, name: str):
    for i in range(wait_sec):
        await asyncio.sleep(1)
        if _ble_control["disconnect_requested"]:
            return None
        if i == wait_sec // 2 and wait_sec > 12:
            await cmd_heartbeat(client)

    if len(_packets) < 2:
        print(f"[{name}] Timeout — only {len(_packets)} packets")
        return None

    buf = bytearray()
    for p in _packets[1:-1]:
        buf += p
    print(f"[{name}] {len(_packets)} packets → {len(buf)} bytes")
    return buf if buf else None


# ═══════════════════════════════════════════════════════════
# CORE FIELD DECODER
# Verified against real sensor capture:
#   offset 0  = amb_temp  (signed byte °C)
#   offset 1  = humidity  (unsigned byte %RH)
#   offset 2  = pres_raw  → hPa = raw*3 + 335  (confirmed vs app)
#   offset 3  = surf_temp (signed byte °C)
#   offset 4  = RPM       (uint16 LE)
#   offset 6  = vib X     (23 bytes)
#   offset 29 = vib Y     (23 bytes)
#   offset 52 = vib Z     (23 bytes)
#   offset 75 = audio_db  (1 byte)
#   offset 76 = audio_freq(4 bytes float)
#   offset 80 = spare     (2 bytes)
#   offset 82 = battery   (1 byte)
# ═══════════════════════════════════════════════════════════

def _read_fields(buf: bytearray, start: int):
    """
    Read all sensor fields starting at absolute offset `start`.
    Returns (next_offset, dict_of_values).
    This is used by BOTH live and history decoders to guarantee
    identical byte interpretation.
    """
    o = start

    # Environmental
    amb_temp  = struct.unpack_from('<b', buf, o)[0]; o += 1   # signed °C
    humidity  = buf[o];                              o += 1   # %RH
    pres_raw  = buf[o];                              o += 1   # raw
    pressure  = (pres_raw * 3) + 335                          # hPa confirmed
    surf_temp = struct.unpack_from('<b', buf, o)[0]; o += 1   # signed °C
    rpm       = struct.unpack_from('<H', buf, o)[0]; o += 2   # rpm

    # Vibration — 23 bytes × 3 axes
    # Per axis: vel(4f) acc(4f) freq(4f) disp(4f) kurt(1) crest(1) skew(1) spare(4)
    vib = {}
    for axis in ['x', 'y', 'z']:
        vel   = struct.unpack_from('<f', buf, o)[0]; o += 4
        acc   = struct.unpack_from('<f', buf, o)[0]; o += 4
        freq  = struct.unpack_from('<f', buf, o)[0]; o += 4
        o    += 4                                               # displacement N/A
        kurt  = buf[o] / 10.0;                       o += 1   # scaled x10
        crest = buf[o] / 10.0;                       o += 1
        skew  = buf[o] / 10.0;                       o += 1
        o    += 4                                               # spare
        vib[axis] = (vel, acc, freq, kurt, crest, skew)

    # Audio — 7 bytes
    audio_db   = buf[o];                              o += 1
    audio_freq = struct.unpack_from('<f', buf, o)[0]; o += 4
    o += 2                                                      # spare

    # Battery
    battery = buf[o]; o += 1

    vals = {
        "amb_temp":   amb_temp,
        "humidity":   humidity,
        "pressure":   pressure,
        "pres_raw":   pres_raw,
        "surf_temp":  surf_temp,
        "rpm":        rpm,
        "vib":        vib,
        "audio_db":   audio_db,
        "audio_freq": audio_freq,
        "battery":    battery,
    }
    return o, vals


# ═══════════════════════════════════════════════════════════
# 2.3.1 — LIVE DATA DECODER
# Header: version(1) + spares(8) + timestamp(4) = 13 bytes
# Then _read_fields from offset 13
# ═══════════════════════════════════════════════════════════

def _decode_live(buf: bytearray):
    o = 0
    try:
        o += 1   # record version
        o += 8   # spares (8 zero bytes confirmed from capture)
        o += 4   # timestamp (we use system time)

        # o is now 13 — confirmed correct from capture
        o, v = _read_fields(buf, o)

        t = datetime.now().strftime("%H:%M:%S")
        with _lock:
            live["time"].append(t)
            live["amb_temp"].append(v["amb_temp"])
            live["surf_temp"].append(v["surf_temp"])
            live["humidity"].append(v["humidity"])
            live["pressure"].append(v["pressure"])
            live["vib_x_vel"].append(round(v["vib"]['x'][0], 4))
            live["vib_y_vel"].append(round(v["vib"]['y'][0], 4))
            live["vib_z_vel"].append(round(v["vib"]['z'][0], 4))
            live["vib_x_acc"].append(round(v["vib"]['x'][1], 4))
            live["vib_y_acc"].append(round(v["vib"]['y'][1], 4))
            live["vib_z_acc"].append(round(v["vib"]['z'][1], 4))
            live["acoustics"].append(v["audio_db"])
            live["battery"].append(v["battery"])
            live["rpm"].append(v["rpm"])
            live["kurt_x"]     = v["vib"]['x'][3]
            live["kurt_y"]     = v["vib"]['y'][3]
            live["kurt_z"]     = v["vib"]['z'][3]
            live["crest_x"]    = v["vib"]['x'][4]
            live["crest_y"]    = v["vib"]['y'][4]
            live["crest_z"]    = v["vib"]['z'][4]
            live["skew_x"]     = v["vib"]['x'][5]
            live["skew_y"]     = v["vib"]['y'][5]
            live["skew_z"]     = v["vib"]['z'][5]
            live["vib_x_freq"] = round(v["vib"]['x'][2], 1)
            live["vib_y_freq"] = round(v["vib"]['y'][2], 1)
            live["vib_z_freq"] = round(v["vib"]['z'][2], 1)
            live["audio_freq"] = round(v["audio_freq"], 1)
            live["last_update"]= t
            live["last_cmd"]   = "0x01 Live Data"

        print(
            f"[{t}][0x01] "
            f"Amb:{v['amb_temp']}°C  Surf:{v['surf_temp']}°C  "
            f"Hum:{v['humidity']}%  Pres:{v['pressure']}hPa(raw={v['pres_raw']})  "
            f"RPM:{v['rpm']}  Bat:{v['battery']}%  Audio:{v['audio_db']}dBSPL\n"
            f"  X: {v['vib']['x'][0]:.4f}mm/s  {v['vib']['x'][1]:.4f}g  "
            f"{v['vib']['x'][2]:.1f}Hz  K={v['vib']['x'][3]}  CF={v['vib']['x'][4]}\n"
            f"  Y: {v['vib']['y'][0]:.4f}mm/s  {v['vib']['y'][1]:.4f}g  "
            f"{v['vib']['y'][2]:.1f}Hz  K={v['vib']['y'][3]}  CF={v['vib']['y'][4]}\n"
            f"  Z: {v['vib']['z'][0]:.4f}mm/s  {v['vib']['z'][1]:.4f}g  "
            f"{v['vib']['z'][2]:.1f}Hz  K={v['vib']['z'][3]}  CF={v['vib']['z'][4]}"
        )

    except Exception as e:
        print(f"[0x01] Decode error at offset {o}: {e}")


# ═══════════════════════════════════════════════════════════
# 2.3.2 — HISTORY DATA DECODER
# Same header as live: version(1) + spares(8) + timestamp(4) = 13 bytes
# Then _read_fields from offset 13
# Then alarm status 90 bytes
# Total per record = 186 bytes
# ═══════════════════════════════════════════════════════════

HIST_RECORD_SIZE = 186

def _decode_history(buf: bytearray):
    records = []
    o = 0

    # Print first record raw bytes for verification
    if len(buf) >= 20:
        print(f"[0x02] Buffer {len(buf)}b  "
              f"First 20 bytes: {buf[:20].hex()}")
        # Verify version byte
        print(f"[0x02] byte[0]={buf[0]} (expect 1=version)  "
              f"byte[13]={buf[13]} (expect ~28=amb_temp)  "
              f"byte[16]={struct.unpack_from('<b',buf,16)[0]} (expect ~29=surf_temp)")

    try:
        while o + HIST_RECORD_SIZE <= len(buf):
            rec_start = o

            # Version byte must be 1
            if buf[o] != 0x01:
                print(f"[0x02] Unexpected version {buf[o]} at offset {o} — skipping byte")
                o += 1
                continue

            o += 1   # version
            o += 8   # spares
            ts = struct.unpack_from('<I', buf, o)[0]; o += 4  # timestamp

            # Read all sensor fields — same function as live data
            o, v = _read_fields(buf, o)

            # Skip alarm status 90 bytes
            o += 90

            # Force exact alignment to next record boundary
            expected_end = rec_start + HIST_RECORD_SIZE
            if o != expected_end:
                print(f"[0x02] Alignment fix: was {o}, forcing to {expected_end}")
                o = expected_end

            # Validate temperature range per spec (-40 to +80°C)
            if not (-40 <= v["amb_temp"] <= 80):
                print(f"[0x02] Invalid amb_temp={v['amb_temp']} — skipping record")
                continue
            if not (-40 <= v["surf_temp"] <= 80):
                print(f"[0x02] Invalid surf_temp={v['surf_temp']} — skipping record")
                continue

            # Format timestamp
            try:
                dt = datetime.fromtimestamp(ts).strftime("%d/%m %H:%M") \
                    if ts > 1_000_000 else "—"
            except Exception:
                dt = "—"

            records.append({
                "time":      dt,
                "amb_temp":  v["amb_temp"],
                "surf_temp": v["surf_temp"],
                "humidity":  v["humidity"],
                "pressure":  v["pressure"],
                "rpm":       v["rpm"],
                "vib_x_vel": round(v["vib"]['x'][0], 4),
                "vib_x_acc": round(v["vib"]['x'][1], 4),
                "kurt_x":    v["vib"]['x'][3],
                "crest_x":   v["vib"]['x'][4],
                "acoustics": v["audio_db"],
                "battery":   v["battery"],
            })

        with _lock:
            live["history"]  = records
            live["last_cmd"] = f"0x02 History ({len(records)} records)"

        print(f"[0x02] {len(records)} records decoded:")
        for r in records:
            print(f"  {r['time']}  Amb:{r['amb_temp']}°C  "
                  f"Surf:{r['surf_temp']}°C  "
                  f"Pres:{r['pressure']}hPa  "
                  f"Hum:{r['humidity']}%  "
                  f"Bat:{r['battery']}%")

    except Exception as e:
        print(f"[0x02] Decode error at offset {o}: {e}")


# ═══════════════════════════════════════════════════════════
# 2.3.3 — RAW VIBRATION DECODER
# ═══════════════════════════════════════════════════════════

def _decode_raw(buf: bytearray, axis_name: str):
    HEADER = 14   # cmd(1)+crc(2)+frame_no(1)+frame_len(2)+sensitivity(4)+spare(4)
    samples = []
    o = 0
    try:
        while o < len(buf):
            if o + HEADER > len(buf):
                break
            if buf[o] != 0x1B:
                o += 1
                continue
            frame_start = o
            o += 1                                                       # cmd id
            o += 2                                                       # CRC16
            o += 1                                                       # frame_no
            frame_len   = struct.unpack_from('<H', buf, o)[0]; o += 2
            sensitivity = struct.unpack_from('<f', buf, o)[0]; o += 4
            o += 4                                                       # spare
            if frame_len < HEADER:
                o = frame_start + 1
                continue
            n = (frame_len - HEADER) // 2
            if o + n * 2 > len(buf):
                break
            for _ in range(n):
                raw_s = struct.unpack_from('<h', buf, o)[0]; o += 2
                samples.append((raw_s * sensitivity) / 1000.0)
            next_f = frame_start + frame_len
            if next_f > o:
                o = next_f
    except Exception as e:
        print(f"[0x1B {axis_name.upper()}] Error at {o}: {e}")

    with _lock:
        live[f"raw_{axis_name}"] = samples
        live["last_cmd"] = f"0x1B Raw {axis_name.upper()} ({len(samples)} samples)"

    print(f"[0x1B {axis_name.upper()}] {len(samples)} samples"
          + (f"  Min:{min(samples):.4f}  Max:{max(samples):.4f}g" if samples else ""))


# ═══════════════════════════════════════════════════════════
# COMMAND RUNNERS
# ═══════════════════════════════════════════════════════════

async def run_live(client):
    _packets.clear()
    await client.write_gatt_char(CHAR_UUID, bytearray([0x01, 0x00]), response=False)
    print("[0x01] Sent")
    buf = await _collect(client, 13, "0x01")
    if buf:
        _decode_live(buf)
    elif not _ble_control["disconnect_requested"]:
        await cmd_abort(client)


async def run_history(client, count=5):
    _packets.clear()
    cmd = bytearray([0x02, 0x01]) + struct.pack('<I', count)
    await client.write_gatt_char(CHAR_UUID, cmd, response=False)
    print(f"[0x02] Sent (count={count})")
    buf = await _collect(client, 15, "0x02")
    if buf:
        _decode_history(buf)
    elif not _ble_control["disconnect_requested"]:
        await cmd_abort(client)


async def run_raw(client, axis: int):
    names = {0: 'x', 1: 'y', 2: 'z'}
    name  = names[axis]
    _packets.clear()
    cmd = bytearray([0x1B]) + struct.pack('<I', 0x04) + struct.pack('<I', axis) + bytearray(3)
    await client.write_gatt_char(CHAR_UUID, cmd, response=False)
    print(f"[0x1B] Sent axis={name.upper()}")
    buf = await _collect(client, 15, f"0x1B {name.upper()}")
    if buf:
        _decode_raw(buf, name)
    elif not _ble_control["disconnect_requested"]:
        await cmd_abort(client)

# ==========================================================
# DUMMY DATA GENERATOR
# ==========================================================
def generate_dummy_data():
    while True:
        t = datetime.now().strftime("%H:%M:%S")

        with _lock:
            live["ble_status"] = "Demo Mode Connected"

            live["time"].append(t)
            live["amb_temp"].append(random.randint(24, 36))
            live["surf_temp"].append(random.randint(30, 50))
            live["humidity"].append(random.randint(40, 90))
            live["pressure"].append(random.randint(980, 1035))

            live["vib_x_vel"].append(round(random.uniform(0.2,2.0),2))
            live["vib_y_vel"].append(round(random.uniform(0.2,2.0),2))
            live["vib_z_vel"].append(round(random.uniform(0.2,2.0),2))

            live["vib_x_acc"].append(round(random.uniform(0.1,1.0),2))
            live["vib_y_acc"].append(round(random.uniform(0.1,1.0),2))
            live["vib_z_acc"].append(round(random.uniform(0.1,1.0),2))

            live["acoustics"].append(random.randint(50,95))
            live["battery"].append(random.randint(60,100))
            live["rpm"].append(random.randint(800,1800))

            live["kurt_x"]=round(random.uniform(2,5),2)
            live["kurt_y"]=round(random.uniform(2,5),2)
            live["kurt_z"]=round(random.uniform(2,5),2)

            live["crest_x"]=round(random.uniform(1,3),2)
            live["crest_y"]=round(random.uniform(1,3),2)
            live["crest_z"]=round(random.uniform(1,3),2)

            live["skew_x"]=round(random.uniform(-1,1),2)
            live["skew_y"]=round(random.uniform(-1,1),2)
            live["skew_z"]=round(random.uniform(-1,1),2)

            live["vib_x_freq"]=random.randint(10,80)
            live["vib_y_freq"]=random.randint(10,80)
            live["vib_z_freq"]=random.randint(10,80)

            live["audio_freq"]=random.randint(200,1000)

            live["last_update"]=t
            live["last_cmd"]="Dummy Data"

        time.sleep(3)
# ═══════════════════════════════════════════════════════════
# BLE LOOP
# ═══════════════════════════════════════════════════════════

async def ble_loop():
    cycle = 0
    while True:
        if not _ble_control["connect_requested"]:
            await asyncio.sleep(1)
            continue

        try:
            with _lock:
                live["ble_status"] = "Connecting..."
            print(f"\nBLE: Connecting to {ADDRESS}...")

            async with BleakClient(ADDRESS) as client:
                _ble_control["running"]              = True
                _ble_control["disconnect_requested"] = False
                with _lock:
                    live["ble_status"] = "Connected"
                print("BLE: Connected\n")

                await client.start_notify(CHAR_UUID, _on_notify)

                while client.is_connected:

                    # Check disconnect request
                    if _ble_control["disconnect_requested"]:
                        await cmd_abort_and_disconnect(client)
                        _ble_control["connect_requested"]    = False
                        _ble_control["disconnect_requested"] = False
                        _ble_control["running"]              = False
                        break

                    # 2.3.1 Live data
                    await run_live(client)
                    if _ble_control["disconnect_requested"]:
                        continue

                    # 2.3.4 Heartbeat
                    await cmd_heartbeat(client)

                    # 2.3.2 History every 5 cycles
                    if cycle % 5 == 0:
                        await run_history(client, count=5)
                        if _ble_control["disconnect_requested"]:
                            continue
                        await cmd_heartbeat(client)

                    # 2.3.3 Raw X every 10 cycles
                    if cycle % 10 == 0:
                        await run_raw(client, axis=0)
                        if _ble_control["disconnect_requested"]:
                            continue
                        await cmd_heartbeat(client)

                    # 2.3.3 Raw Y every 15 cycles
                    if cycle % 15 == 0:
                        await run_raw(client, axis=1)
                        if _ble_control["disconnect_requested"]:
                            continue
                        await cmd_heartbeat(client)

                    # 2.3.3 Raw Z every 20 cycles
                    if cycle % 20 == 0:
                        await run_raw(client, axis=2)
                        if _ble_control["disconnect_requested"]:
                            continue
                        await cmd_heartbeat(client)

                    cycle += 1
                    await asyncio.sleep(2)
                    
        except Exception as e:
            with _lock:
                live["ble_status"] = "Disconnected"
            _ble_control["running"] = False
            if _ble_control["disconnect_requested"]:
                _ble_control["connect_requested"]    = False
                _ble_control["disconnect_requested"] = False
            else:
                print(f"BLE error: {e}")
                print("Starting Dummy Data Mode...")
                threading.Thread(target=generate_dummy_data, daemon=True).start()
                return


def run_ble():
    asyncio.run(ble_loop())


def _shutdown(sig, frame):
    print("\nShutting down...")
    sys.exit(0)

signal.signal(signal.SIGINT, _shutdown)
threading.Thread(target=generate_dummy_data, daemon=True).start()


# ═══════════════════════════════════════════════════════════
# DASH APP
# ═══════════════════════════════════════════════════════════

app = dash.Dash(__name__)
app.title = "Versatilis Live"

C = {
    "bg":     "#0d0d14", "card":   "#13131f", "border": "#1e1e2e",
    "purple": "#cba6f7", "blue":   "#89b4fa", "green":  "#a6e3a1",
    "red":    "#f38ba8", "yellow": "#f9e2af", "teal":   "#89dceb",
    "text":   "#cdd6f4", "muted":  "#6c7086",
}

CARD = {"background": C["card"], "border": f"1px solid {C['border']}",
        "borderRadius": "12px", "padding": "18px"}

TH = {"padding": "6px 14px", "color": C["muted"], "fontSize": "10px",
      "borderBottom": f"1px solid {C['border']}", "textAlign": "left",
      "letterSpacing": "0.06em", "textTransform": "uppercase"}
TD = {"padding": "6px 14px", "color": C["text"],
      "fontSize": "12px", "fontFamily": "monospace"}
TA = {"padding": "6px 14px", "color": C["purple"],
      "fontSize": "12px", "fontFamily": "monospace", "fontWeight": "bold"}
BTN = {"padding": "8px 20px", "borderRadius": "8px", "border": "none",
       "fontFamily": "monospace", "fontSize": "12px", "cursor": "pointer",
       "fontWeight": "500"}


def sensor_card(label, val_id, unit, color):
    return html.Div(style={**CARD, "flex": "1", "minWidth": "120px"}, children=[
        html.Div(label, style={"fontSize": "10px", "color": C["muted"],
                               "fontFamily": "monospace", "letterSpacing": "0.08em",
                               "textTransform": "uppercase", "marginBottom": "6px"}),
        html.Div(id=val_id, style={"fontSize": "30px", "fontWeight": "700",
                                    "color": color, "fontFamily": "monospace",
                                    "lineHeight": "1"}),
        html.Div(unit, style={"fontSize": "11px", "color": C["muted"],
                              "fontFamily": "monospace", "marginTop": "4px"}),
    ])


def plot_cfg(ytitle=""):
    return dict(
        paper_bgcolor=C["card"], plot_bgcolor=C["card"],
        margin=dict(l=50, r=20, t=10, b=40),
        font=dict(color=C["text"], family="monospace", size=11),
        legend=dict(font=dict(color=C["text"], size=11),
                    bgcolor="rgba(0,0,0,0)", borderwidth=0),
        xaxis=dict(color=C["muted"], showgrid=False, tickfont=dict(size=10)),
        yaxis=dict(color=C["text"], gridcolor=C["border"],
                   gridwidth=0.5, tickfont=dict(size=10), title=ytitle),
    )


def trend_fig(t, series):
    if not t:
        f = go.Figure(); f.update_layout(**plot_cfg()); return f
    f = go.Figure([go.Scatter(x=list(t), y=list(s[0]), name=s[1],
                               mode="lines", line=dict(color=s[2], width=2))
                   for s in series])
    f.update_layout(**plot_cfg())
    return f


def raw_fig(rx, ry, rz):
    f = go.Figure(); f.update_layout(**plot_cfg("g"))
    has = False
    for samples, color, label in [
        (rx, C["blue"], "X"), (ry, C["green"], "Y"), (rz, C["red"], "Z")
    ]:
        if samples:
            has = True
            step = max(1, len(samples) // 2000)
            f.add_trace(go.Scatter(
                x=list(range(0, len(samples), step)),
                y=samples[::step], mode="lines",
                line=dict(color=color, width=1), name=label
            ))
    if not has:
        f.add_annotation(text="Waiting for raw vibration data...",
                         xref="paper", yref="paper", x=0.5, y=0.5,
                         showarrow=False, font=dict(color=C["muted"], size=12))
    return f


app.layout = html.Div(
    style={"background": C["bg"], "minHeight": "100vh",
           "padding": "20px 24px", "fontFamily": "monospace"},
    children=[

        # Header
        html.Div(style={"display": "flex", "justifyContent": "space-between",
                        "alignItems": "flex-start", "marginBottom": "18px",
                        "flexWrap": "wrap", "gap": "12px"}, children=[
            html.Div(children=[
                html.H2("Honeywell Versatilis — Live Monitor",
                        style={"color": C["purple"], "margin": "0 0 6px 0",
                               "fontSize": "20px", "fontWeight": "500"}),
                html.Div(style={"display": "flex", "gap": "20px",
                                "fontSize": "12px", "flexWrap": "wrap"}, children=[
                    html.Span(id="s-status"),
                    html.Span(id="s-update", style={"color": C["muted"]}),
                    html.Span(id="s-cmd",    style={"color": C["muted"]}),
                ]),
            ]),
            html.Div(style={"display": "flex", "gap": "10px",
                            "alignItems": "center"}, children=[
                html.Button("Connect", id="btn-con", n_clicks=0,
                            style={**BTN, "background": C["green"],
                                   "color": "#173404"}),
                html.Button("Disconnect  (0x16)", id="btn-dis", n_clicks=0,
                            style={**BTN, "background": C["red"],
                                   "color": "#501313"}),
                html.Span(id="btn-msg",
                          style={"color": C["muted"], "fontSize": "11px"}),
            ]),
        ]),

        # Sensor cards
        html.Div(style={"display": "flex", "gap": "10px",
                        "marginBottom": "14px", "flexWrap": "wrap"}, children=[
            sensor_card("Ambient temp",  "v-amb",   "°C",    C["blue"]),
            sensor_card("Surface temp",  "v-surf",  "°C",    C["red"]),
            sensor_card("Humidity",      "v-hum",   "%RH",   C["teal"]),
            sensor_card("Pressure",      "v-pres",  "hPa",   C["green"]),
            sensor_card("Battery",       "v-bat",   "%",     C["yellow"]),
            sensor_card("RPM",           "v-rpm",   "rpm",   C["purple"]),
            sensor_card("Acoustics",     "v-audio", "dBSPL", C["teal"]),
        ]),

        # Vib velocity
        html.Div(style={**CARD, "marginBottom": "14px"}, children=[
            html.Div("Vibration velocity — X / Y / Z (mm/s)",
                     style={"color": C["text"], "fontSize": "12px",
                            "marginBottom": "8px"}),
            dcc.Graph(id="g-vvel", config={"displayModeBar": False},
                      style={"height": "200px"}),
        ]),

        # Vib acceleration
        html.Div(style={**CARD, "marginBottom": "14px"}, children=[
            html.Div("Vibration acceleration — X / Y / Z (g)",
                     style={"color": C["text"], "fontSize": "12px",
                            "marginBottom": "8px"}),
            dcc.Graph(id="g-vacc", config={"displayModeBar": False},
                      style={"height": "200px"}),
        ]),

        # Temp + Audio
        html.Div(style={"display": "flex", "gap": "14px",
                        "marginBottom": "14px"}, children=[
            html.Div(style={**CARD, "flex": "1"}, children=[
                html.Div("Temperature trend (°C)",
                         style={"color": C["text"], "fontSize": "12px",
                                "marginBottom": "8px"}),
                dcc.Graph(id="g-temp", config={"displayModeBar": False},
                          style={"height": "180px"}),
            ]),
            html.Div(style={**CARD, "flex": "1"}, children=[
                html.Div("Acoustics trend (dBSPL)",
                         style={"color": C["text"], "fontSize": "12px",
                                "marginBottom": "8px"}),
                dcc.Graph(id="g-audio", config={"displayModeBar": False},
                          style={"height": "180px"}),
            ]),
        ]),

        # Health table
        html.Div(style={**CARD, "marginBottom": "14px"}, children=[
            html.Div("Vibration health indicators  (2.3.1)",
                     style={"color": C["text"], "fontSize": "12px",
                            "marginBottom": "12px"}),
            html.Div(id="t-health"),
        ]),

        # History table
        html.Div(style={**CARD, "marginBottom": "14px"}, children=[
            html.Div("Sensor history — last 5 records  (2.3.2)",
                     style={"color": C["text"], "fontSize": "12px",
                            "marginBottom": "12px"}),
            html.Div(id="t-history"),
        ]),

        # Raw vibration
        html.Div(style={**CARD, "marginBottom": "14px"}, children=[
            html.Div("Raw vibration waveform — X / Y / Z  (2.3.3)",
                     style={"color": C["text"], "fontSize": "12px",
                            "marginBottom": "8px"}),
            dcc.Graph(id="g-raw", config={"displayModeBar": False},
                      style={"height": "220px"}),
        ]),

        dcc.Interval(id="iv", interval=3000, n_intervals=0),
    ]
)


# ─── Button callback ──────────────────────────────────────

@app.callback(
    Output("btn-msg", "children"),
    [Input("btn-con", "n_clicks"), Input("btn-dis", "n_clicks")],
    prevent_initial_call=True
)
def on_button(nc, nd):
    from dash import ctx
    tid = ctx.triggered_id
    if tid == "btn-con":
        if _ble_control["running"]:
            return "Already connected"
        _ble_control["connect_requested"]    = True
        _ble_control["disconnect_requested"] = False
        return "Connecting..."
    if tid == "btn-dis":
        if not _ble_control["running"]:
            return "Not connected"
        _ble_control["disconnect_requested"] = True
        return "Sending 0x16 abort..."
    return ""


# ─── Main data callback ───────────────────────────────────

@app.callback(
    [Output("s-status",  "children"), Output("s-update",  "children"),
     Output("s-cmd",     "children"), Output("v-amb",      "children"),
     Output("v-surf",    "children"), Output("v-hum",      "children"),
     Output("v-pres",    "children"), Output("v-bat",      "children"),
     Output("v-rpm",     "children"), Output("v-audio",    "children"),
     Output("g-vvel",    "figure"),   Output("g-vacc",     "figure"),
     Output("g-temp",    "figure"),   Output("g-audio",    "figure"),
     Output("t-health",  "children"), Output("t-history",  "children"),
     Output("g-raw",     "figure")],
    Input("iv", "n_intervals")
)
def update(_):
    with _lock:
        st   = live["ble_status"]
        t    = list(live["time"])
        at   = list(live["amb_temp"])
        sft  = list(live["surf_temp"])
        hum  = list(live["humidity"])
        pres = list(live["pressure"])
        xv   = list(live["vib_x_vel"]); yv = list(live["vib_y_vel"]); zv = list(live["vib_z_vel"])
        xa   = list(live["vib_x_acc"]); ya = list(live["vib_y_acc"]); za = list(live["vib_z_acc"])
        acu  = list(live["acoustics"])
        bat  = list(live["battery"])
        rpm  = list(live["rpm"])
        kx,ky,kz   = live["kurt_x"],  live["kurt_y"],  live["kurt_z"]
        cx,cy,cz   = live["crest_x"], live["crest_y"], live["crest_z"]
        sx,sy,sz   = live["skew_x"],  live["skew_y"],  live["skew_z"]
        fx,fy,fz   = live["vib_x_freq"], live["vib_y_freq"], live["vib_z_freq"]
        hist  = list(live["history"])
        rx,ry,rz   = list(live["raw_x"]), list(live["raw_y"]), list(live["raw_z"])
        upd   = live["last_update"]
        cmd   = live["last_cmd"]

    ok = st == "Connected"
    status = html.Span(f"● {st}",
                       style={"color": C["green"] if ok else
                              C["yellow"] if "Conn" in st else C["red"]})

    health = html.Table(
        style={"width": "100%", "borderCollapse": "collapse"},
        children=[
            html.Thead(html.Tr([html.Th(h, style=TH) for h in
                                ["Axis","Kurtosis","Crest factor",
                                 "Skewness","Dom. frequency"]])),
            html.Tbody([
                html.Tr([html.Td(f"Axis {a}", style=TA),
                         html.Td(str(k), style=TD), html.Td(str(c), style=TD),
                         html.Td(str(s), style=TD), html.Td(f"{f} Hz", style=TD)])
                for a,k,c,s,f in [("X",kx,cx,sx,fx),("Y",ky,cy,sy,fy),("Z",kz,cz,sz,fz)]
            ]),
        ]
    )

    if hist:
        hrows = [
            html.Tr([
                html.Td(r["time"],                style=TD),
                html.Td(f"{r['amb_temp']}°C",     style=TD),
                html.Td(f"{r['surf_temp']}°C",    style=TD),
                html.Td(f"{r['pressure']} hPa",   style=TD),
                html.Td(f"{r['humidity']}%",      style=TD),
                html.Td(f"{r['vib_x_vel']} mm/s", style=TD),
                html.Td(str(r["kurt_x"]),         style=TD),
                html.Td(str(r["crest_x"]),        style=TD),
                html.Td(f"{r['acoustics']} dBSPL",style=TD),
                html.Td(f"{r['battery']}%",       style=TD),
            ])
            for r in hist
        ]
    else:
        hrows = [html.Tr([html.Td(
            "Fetching — runs every 5 cycles (~75s after start)...",
            colSpan=10, style={**TD, "color": C["muted"]})])]

    htable = html.Table(
        style={"width": "100%", "borderCollapse": "collapse"},
        children=[
            html.Thead(html.Tr([html.Th(h, style=TH) for h in
                                ["Time","Amb","Surf","Pressure","Humidity",
                                 "Vib X","Kurtosis","Crest","Audio","Battery"]])),
            html.Tbody(hrows),
        ]
    )

    L = lambda d: str(d[-1]) if d else "—"

    return (
        status, f"Last update: {upd}", f"Last cmd: {cmd}",
        L(at), L(sft), L(hum), L(pres), L(bat), L(rpm), L(acu),
        trend_fig(t, [(xv,"X",C["blue"]),(yv,"Y",C["green"]),(zv,"Z",C["red"])]),
        trend_fig(t, [(xa,"X",C["blue"]),(ya,"Y",C["green"]),(za,"Z",C["red"])]),
        trend_fig(t, [(at,"Ambient",C["blue"]),(sft,"Surface",C["red"])]),
        trend_fig(t, [(acu,"dBSPL",C["teal"])]),
        health, htable, raw_fig(rx, ry, rz),
    )


if __name__ == "__main__":
    print("Dashboard starting → http://localhost:8050")
    app.run(debug=False, port=8050)
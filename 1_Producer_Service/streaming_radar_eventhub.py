# streaming_radar_producer.py
import os
import json
import time
import random
import uuid
from math import radians, cos, sin, asin, sqrt
from datetime import datetime, timedelta

from faker import Faker
from azure.eventhub import EventHubProducerClient, EventData

fake = Faker()

# ----------------------------
# Config
# ----------------------------

# Azure Event Hub config

EVENTHUB_CONNECTION_STRING = os.environ.get("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.environ.get("EVENTHUB_NAME")
ENABLE_EVENTHUB = os.environ.get("ENABLE_EVENTHUB", "true").lower() == "true"

# Simulation config
TICK_INTERVAL = float(os.environ.get("TICK_INTERVAL", 1.0))
SPAWN_RATE_PER_MIN = float(os.environ.get("SPAWN_RATE_PER_MIN", 8))
MAX_ACTIVE_JOURNEYS = int(os.environ.get("MAX_ACTIVE_JOURNEYS", 30))
CONTINUE_TO_NEXT_ROUTE_PROB = float(os.environ.get("CONTINUE_TO_NEXT_ROUTE_PROB", 0.25))


# Simulation utilities
ROUTES = [
    {"route_id": "Route_1", "name": "Cairo-Alex Road", "speed_limit": 120, "center": (30.0444, 31.2357), "radar_count": 6},
    {"route_id": "Route_2", "name": "Ring Road East", "speed_limit": 100, "center": (30.0500, 31.3000), "radar_count": 5},
    {"route_id": "Route_3", "name": "Corniche Road", "speed_limit": 80,  "center": (31.2000, 29.9167), "radar_count": 7},
    {"route_id": "Route_4", "name": "Downtown Circuit", "speed_limit": 60,  "center": (30.0333, 31.2333), "radar_count": 4},
    {"route_id": "Route_5", "name": "Airport Road", "speed_limit": 100, "center": (30.1210, 31.4050), "radar_count": 5},
]

def generate_radars_for_routes(routes):
    for r in routes:
        lat0, lon0 = r["center"]
        n = r.get("radar_count", 4)
        radars = []
        for i in range(n):
            frac = i / max(1, n - 1)
            lat = lat0 + (frac - 0.5) * 0.06 + random.uniform(-0.005, 0.005)
            lon = lon0 + (frac - 0.5) * 0.06 + random.uniform(-0.005, 0.005)
            radars.append({"radar_index": i + 1, "lat": round(lat, 6), "lon": round(lon, 6)})
        r["radars"] = radars

generate_radars_for_routes(ROUTES)

DRIVER_PROFILES = {
    "safe": {"pct": 0.6, "variance": (-10, 5), "seatbelt_prob": 0.98, "phone_prob": 0.02},
    "average": {"pct": 0.3, "variance": (-5, 15), "seatbelt_prob": 0.85, "phone_prob": 0.12},
    "aggressive": {"pct": 0.1, "variance": (10, 40), "seatbelt_prob": 0.60, "phone_prob": 0.30},
}

VIOLATION_TYPES = {
    "SPD_MODERATE": {"code": "SPD_001", "name": "Moderate Speeding", "fine": 500},
    "SPD_SEVERE": {"code": "SPD_002", "name": "Severe Speeding", "fine": 1000},
    "NO_SEATBELT": {"code": "SBL_001", "name": "No Seat Belt", "fine": 300},
    "PHONE_USE": {"code": "PHN_001", "name": "Phone Usage", "fine": 400},
}

def choose_driver_profile():
    r = random.random()
    cumulative = 0
    for profile, meta in DRIVER_PROFILES.items():
        cumulative += meta["pct"]
        if r <= cumulative:
            return profile
    return "average"

def generate_plate():
    letters = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3))
    digits = random.randint(100, 9999)
    return f"{letters}-{digits}"

def generate_speed(speed_limit, profile):
    var_min, var_max = DRIVER_PROFILES[profile]["variance"]
    speed = speed_limit + random.randint(var_min, var_max)
    return max(20, min(220, speed))

def detect_violations(speed, speed_limit, seat_belt, phone_usage):
    violations = []
    total_fine = 0
    if speed > speed_limit + 20:
        v = VIOLATION_TYPES["SPD_SEVERE"]
        violations.append(v["code"])
        total_fine += v["fine"]
    elif speed > speed_limit:
        v = VIOLATION_TYPES["SPD_MODERATE"]
        violations.append(v["code"])
        total_fine += v["fine"]
    if not seat_belt:
        v = VIOLATION_TYPES["NO_SEATBELT"]
        violations.append(v["code"])
        total_fine += v["fine"]
    if phone_usage:
        v = VIOLATION_TYPES["PHONE_USE"]
        violations.append(v["code"])
        total_fine += v["fine"]
    return violations, total_fine

def haversine_km(lat1, lon1, lat2, lon2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6371 * c
    return km

# ---------- Journey model ----------
class Journey:
    def __init__(self, journey_id, route):
        self.journey_id = journey_id
        self.route = route
        self.route_id = route["route_id"]
        self.radars = route["radars"]
        self.current_index = 0
        self.plate = generate_plate()
        self.color = random.choice(["white", "black", "silver", "blue", "red", "gray", "green"])
        self.driver_profile = choose_driver_profile()
        self.speed = generate_speed(route["speed_limit"], self.driver_profile)
        self.seat_belt = random.random() < DRIVER_PROFILES[self.driver_profile]["seatbelt_prob"]
        self.phone_usage = random.random() < DRIVER_PROFILES[self.driver_profile]["phone_prob"]
        self.start_time = datetime.utcnow()
        self.next_arrival_time = self.start_time
        self.ended = False
        self.total_distance = 0.0
        self.total_violations = 0
        self.total_fines = 0

    def advance_to_next_radar(self):
        if self.current_index >= len(self.radars) - 1:
            self.ended = True
            return None
        a = self.radars[self.current_index]
        b = self.radars[self.current_index + 1]
        dist_km = haversine_km(a["lat"], a["lon"], b["lat"], b["lon"])
        travel_hours = dist_km / max(1e-6, self.speed)
        travel_seconds = travel_hours * 3600
        jitter = random.uniform(-0.15, 0.3) * travel_seconds
        arrival = datetime.utcnow() + timedelta(seconds=max(1, travel_seconds + jitter))
        return {"arrival": arrival, "segment_distance_km": round(dist_km, 4)}

    def to_record_at_radar(self, radar_idx, segment_distance_km=None):
        radar = self.radars[radar_idx]
        violations, total_fine = detect_violations(self.speed, self.route["speed_limit"], self.seat_belt, self.phone_usage)
        if segment_distance_km:
            self.total_distance += segment_distance_km
        if violations:
            self.total_violations += 1
            self.total_fines += total_fine
        
        # هذا هو شكل الرسالة التي سترسل إلى Event Hub
        return {
            "id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "journey_id": self.journey_id,
            "plate": self.plate,
            "color": self.color,
            "driver_profile": self.driver_profile,
            "route_id": self.route_id,
            "radar_id": f"{self.route_id}_Radar_{radar['radar_index']}",
            "radar_index": radar["radar_index"],
            "lat": radar["lat"],
            "lon": radar["lon"],
            "speed": int(self.speed),
            "speed_limit": self.route["speed_limit"],
            "seat_belt": self.seat_belt,
            "phone_usage": self.phone_usage,
            "is_violation": len(violations) > 0,
            "violation_codes": ";".join(violations),
            "total_fine": total_fine,
            "segment_distance_km": segment_distance_km if segment_distance_km is not None else None
        }


def send_to_eventhub(producer, record):
    """إرسال رسالة لـ Event Hub باستخدام Producer موجود"""
    try:
        # تحويل إلى JSON
        message_json = json.dumps(record, ensure_ascii=False)
        
        batch = producer.create_batch()
        batch.add(EventData(message_json))
        producer.send_batch(batch)
        
        return True, "✅ EventHub"
        
    except Exception as e:
        return False, f"❌ EventHub: {str(e)[:50]}"

# ---------- Main streaming generator ----------

def streaming_journeys_forever():
    print("🚦 Starting Clean Radar Producer (Event Hub Only)... (Ctrl+C to stop)\n")

    producer = None

    eventhub_enabled = ENABLE_EVENTHUB

    if eventhub_enabled:
        if not EVENTHUB_CONNECTION_STRING or not EVENTHUB_NAME:
            print("❌ Error: EVENTHUB_CONNECTION_STRING and EVENTHUB_NAME must be set.")
            eventhub_enabled = False # 3. تعديل المتغير المحلي آمن
        else:
            try:
                # إنشاء الـ Producer مرة واحدة
                producer = EventHubProducerClient.from_connection_string(
                    conn_str=EVENTHUB_CONNECTION_STRING,
                    eventhub_name=EVENTHUB_NAME
                )
                print("✅ Event Hub Producer is ready.")
            except Exception as e:
                print(f"❌ Failed to create Event Hub Producer: {e}")
                eventhub_enabled = False


    active = []
    journey_seq = 0
    spawn_interval_seconds = 60.0 / max(1e-6, SPAWN_RATE_PER_MIN)
    last_spawn_time = datetime.utcnow()

    try:
        while True:
            now = datetime.utcnow()

            # Spawn new journeys
            if (now - last_spawn_time).total_seconds() >= spawn_interval_seconds and len(active) < MAX_ACTIVE_JOURNEYS:
                journey_seq += 1
                route = random.choice(ROUTES)
                j = Journey(f"journey_{journey_seq}", route)
                j.next_arrival_time = now
                active.append(j)
                last_spawn_time = now
                print(f"🚗 New journey started: {j.journey_id} - {j.plate}")

            # Process active journeys
            to_remove = []
            for j in active:
                if j.next_arrival_time <= now:
                    # حساب المسافة للقطعة الحالية
                    segment_distance = None
                    if j.current_index > 0:
                        prev_radar = j.radars[j.current_index - 1]
                        curr_radar = j.radars[j.current_index]
                        segment_distance = haversine_km(prev_radar["lat"], prev_radar["lon"], curr_radar["lat"], curr_radar["lon"])

                    record = j.to_record_at_radar(j.current_index, segment_distance)

                    # إرسال إلى Event Hub
                    event_status = "⏸️ EventHub"
                    if eventhub_enabled and producer: 
                        event_success, event_status = send_to_eventhub(producer, record)

                    # طباعة الحالة (الآن نطبع حالة Event Hub فقط)
                    print(f"📨 Journey {record['journey_id']} | {record['plate']} | Speed: {record['speed']}/{record['speed_limit']} | {event_status}")

                    # Advance to next radar
                    if j.current_index < len(j.radars) - 1:
                        info = j.advance_to_next_radar()
                        if info:
                            j.current_index += 1
                            j.next_arrival_time = info["arrival"]
                            j.speed = max(20, min(220, j.speed + random.randint(-5, 6)))
                        else:
                            j.ended = True
                    else:
                        j.ended = True

                if j.ended:
                    if random.random() < CONTINUE_TO_NEXT_ROUTE_PROB:
                        other_routes = [r for r in ROUTES if r["route_id"] != j.route_id]
                        if other_routes:
                            new_route = random.choice(other_routes)
                            journey_seq += 1
                            cont = Journey(f"journey_{journey_seq}", new_route)
                            cont.plate = j.plate
                            cont.color = j.color
                            cont.driver_profile = j.driver_profile
                            cont.speed = generate_speed(new_route["speed_limit"], cont.driver_profile)
                            cont.next_arrival_time = now + timedelta(seconds=1)
                            active.append(cont)
                    to_remove.append(j)

            for r in to_remove:
                if r in active:
                    active.remove(r)

            time.sleep(TICK_INTERVAL)

    except KeyboardInterrupt:
        print("\n🛑 Stream stopped by user.")
    except Exception as e:
        print(f"💥 Unexpected error in main loop: {e}")
    finally:
        if producer:
            producer.close()
            print("✅ Event Hub Producer closed.")

# ---------- run ----------
if __name__ == "__main__":
    streaming_journeys_forever()
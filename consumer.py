import json
import sys

from pymongo import MongoClient
from config import MONGO_URI, MONGO_DATABASE, MONGO_COLLECTION


def main():
    # Connect to MongoDB Atlas
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    collection = db[MONGO_COLLECTION]

    print(
        f"[consumer] Connected to MongoDB: {MONGO_DATABASE}.{MONGO_COLLECTION}",
        file=sys.stderr,
        flush=True,
    )

    print(
        "[consumer] Waiting for trip events...",
        file=sys.stderr,
        flush=True,
    )

    count = 0
    inserted = 0

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue

        # Insert event into MongoDB Atlas
        try:
            collection.insert_one(event)
            inserted += 1
        except Exception as e:
            print(
                f"[consumer] MongoDB insert error: {e}",
                file=sys.stderr,
                flush=True,
            )
            continue

        count += 1

        # Display event in terminal
        print("\n" + "=" * 78)
        print(f"🚆 TRIP EVENT #{count}")
        print("=" * 78)
        print(f"Date                 : {event['trip_date']}")
        print(f"Train                : {event['train_number']} - {event['train_name']}")
        print(f"Route                : {event['origin']} → {event['destination']}")
        print(f"Trip # for train     : {event['trip_number_for_train']}")
        print(f"Distance             : {event['distance_km']} km")
        print(f"Scheduled duration   : {event['scheduled_duration_hours']} hours")
        print(f"Average speed        : {event['average_speed_kmph']} km/h")
        print(f"Intermediate stops   : {event['intermediate_stops']}")
        print(f"Total ascent         : {event['total_ascent_m']} m")
        print(f"Total descent        : {event['total_descent_m']} m")
        print(f"Max elevation        : {event['max_elevation_m']} m")
        print(f"Max downhill drop    : {event['max_downhill_drop_m']} m")
        print(f"Brake-stress score   : {event['brake_stress_score_0_100']}/100")
        print(f"Trip risk band       : {event['risk_band']}")
        print(f"Cumulative trips     : {event['cumulative_trips_for_train']}")
        print(f"Cumulative distance  : {event['cumulative_distance_km']} km")
        print(
            f"Cumulative exposure : "
            f"{event['cumulative_brake_stress_exposure']}"
        )
        print("NOTE                 : Proxy model, NOT actual brake pressure/wear.")
        print("=" * 78)

    print(
        f"[consumer] Stream ended. Received {count} trip events.",
        file=sys.stderr,
        flush=True,
    )

    print(
        f"[consumer] Successfully inserted {inserted} events into MongoDB Atlas.",
        file=sys.stderr,
        flush=True,
    )

    client.close()


if __name__ == "__main__":
    main()

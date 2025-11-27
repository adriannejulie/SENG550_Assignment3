import redis
import time

r = redis.Redis(host='localhost', port=6379, db=0)

print("Monitoring Redis key 'order_id'... Press Ctrl+C to stop.")

previous_value = None

try:
    while True:
        value = r.get('order_id')
        if value is not None:
            value = int(value)
        if value != previous_value:
            print(f"Last processed folder/day: {value}")
            previous_value = value
        time.sleep(2)  # checks every 2 seconds
except KeyboardInterrupt:
    print("\nStopped monitoring Redis.")


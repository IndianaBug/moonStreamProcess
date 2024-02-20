import threading
import time
from datetime import datetime

run_once = False

def my_function():
    print("hello, pasha")

def run_at_specific_time(stop_event):
    while True:
        if stop_event.is_set():  # Check for stop event regularly
            return  # Thread exits gracefully

        current_second = datetime.now().second
        if current_second > 58:
            my_function()
        time.sleep(1)

stop_event = threading.Event()
timer_thread = threading.Thread(target=run_at_specific_time, args=(stop_event,))
timer_thread.start()

try:
    # Main thread logic, e.g., user interaction
    while True:
        timer_thread

except KeyboardInterrupt:
    print("Received Ctrl+C, stopping threads...")
    stop_event.set()  # Signal threads to stop

# Wait for timer thread to finish, ensuring clean termination
timer_thread.join()

print("All threads finished, program exiting.")
import threading
import time
from datetime import datetime

def my_function():
    print("hello, pasha")

def run_at_specific_time():
    try:
        while True:
            current_second = datetime.now().second
            if current_second > 58:
                my_function()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Exiting the program.")

timer_thread = threading.Thread(target=run_at_specific_time)
timer_thread.start()
timer_thread.join()
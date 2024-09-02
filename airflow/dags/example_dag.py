from datetime import datetime
import time
from airflow.decorators import dag, task

@dag(dag_id="time_teller", description="Tells time with duration", schedule_interval=None, tags=["background task"])
def time_teller_dag():
    """
    Time teller DAG
    """

    @task
    def generate_greeting():
        """Generate a greeting message."""
        greeting = "Hello, welcome to the Time teller DAG!"
        print(greeting)
        return greeting

    @task
    def print_current_time():
        """Print the current time."""
        current_time = datetime.now()
        print(f"Current time: {current_time}")
        return current_time

    @task
    def sleep_and_print_times():
        """Sleep for 3 seconds and print the start and end times."""
        start_time = datetime.now()
        print(f"Start time: {start_time}")
        time.sleep(3)  # Sleep for 3 seconds
        end_time = datetime.now()
        print(f"End time: {end_time}")
        return {"start_time": start_time, "end_time": end_time}

    @task
    def calculate_duration(times):
        """Calculate and print the duration between start and end times."""
        start_time = times['start_time']
        end_time = times['end_time']
        duration = end_time - start_time
        print(f"Duration: {duration}")
        return duration

    # Define the task dependencies
    greeting = generate_greeting()
    current_time = print_current_time()
    times = sleep_and_print_times()
    calculate_duration(times)

# Initialize the DAG
time_teller_dag = time_teller_dag()
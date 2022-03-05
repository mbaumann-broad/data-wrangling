import functools
import threading
import time

from abc import ABC, abstractmethod
from datetime import datetime
from threading import currentThread, Thread

import schedule


class Scheduler:
    def __init__(self):
        self.stop_run_continuously = None

    @staticmethod
    def run_continuously(interval=1):
        """Continuously run, while executing pending jobs at each
        elapsed time interval.
        @return cease_continuous_run: threading. Event which can
        be set to cease continuous run. Please note that it is
        *intended behavior that run_continuously() does not run
        missed jobs*. For example, if you've registered a job that
        should run every minute, and you set a continuous run
        interval of one hour then your job won't be run 60 times
        at each interval but only once.
        """
        cease_continuous_run = threading.Event()

        class ScheduleThread(threading.Thread):

            def run(self):
                while not cease_continuous_run.is_set():
                    schedule.run_pending()
                    time.sleep(interval)

        continuous_thread = ScheduleThread()
        continuous_thread.start()
        return cease_continuous_run

    @staticmethod
    def run_threaded(job_func):
        job_thread = Thread(target=job_func)
        job_thread.start()

    def start_monitoring(self):
        # Start the background thread
        self.stop_run_continuously = self.run_continuously()

    def stop_monitoring(self):
        # Stop the background thread
        self.stop_run_continuously.set()


class ResponseTimeMonitor(Scheduler):
    interval_seconds = 10

    def catch_exceptions(cancel_on_failure=False):
        def catch_exceptions_decorator(job_func):
            @functools.wraps(job_func)
            def wrapper(*args, **kwargs):
                # noinspection PyBroadException
                try:
                    return job_func(*args, **kwargs)
                except:
                    import traceback
                    print(traceback.format_exc())
                    if cancel_on_failure:
                        return schedule.CancelJob

            return wrapper

        return catch_exceptions_decorator

    class ResponseTimeReporter(ABC):
        def __init__(self, output_filename):
            self.output_filename = output_filename

        @abstractmethod
        def measure_response_time(self) -> float:
            pass

        def measure_and_report(self):
            start_time = datetime.now()
            response_time = self.measure_response_time()
            with open(self.output_filename, "a") as fh:
                fh.write(f"{start_time},{response_time}\n")

    class BondResponseTimeReporter(ResponseTimeReporter):
        def __init__(self, output_filename):
            super().__init__(output_filename)

        def measure_response_time(self) -> float:
            # TODO Measure Bond response time
            print(f"{datetime.now()} checking bond response time on {currentThread().name}")
            return 1.1


    class FenceResponseTimeReporter(ResponseTimeReporter):
        def __init__(self, output_filename):
            super().__init__(output_filename)

        def measure_response_time(self) -> float:
            # TODO Measure Fence response time
            print(f"{datetime.now()} checking fence response time on {currentThread().name}")
            return 2.2

    @catch_exceptions()
    def check_bond_response_time(self):
        output_filename = "bond_response_times.csv"
        reporter = self.BondResponseTimeReporter(output_filename)
        reporter.measure_and_report()
        
    @catch_exceptions()
    def check_fence_response_time(self):
        output_filename = "fence_response_times.csv"
        reporter = self.FenceResponseTimeReporter(output_filename)
        reporter.measure_and_report()

    def configure_monitoring(self):
        schedule.every(self.interval_seconds).seconds.do(super().run_threaded, self.check_bond_response_time)
        schedule.every(self.interval_seconds).seconds.do(super().run_threaded, self.check_fence_response_time)


responseTimeMonitor = ResponseTimeMonitor()

# Configure and start monitoring
responseTimeMonitor.configure_monitoring()
responseTimeMonitor.start_monitoring()

# Run for a while
print("Starting sleep ...")
time.sleep(60)
print("Done sleeping")

# End monitoring
responseTimeMonitor.stop_monitoring()

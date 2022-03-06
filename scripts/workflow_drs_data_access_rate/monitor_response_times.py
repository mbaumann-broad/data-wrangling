import functools
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime
from threading import currentThread, Thread
from typing import Tuple

import requests
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
        def measure_response_time(self) -> Tuple[float, int]:
            pass

        def measure_and_report(self):
            start_time = datetime.now()
            response_time, status_code = self.measure_response_time()
            response_time = round(response_time, 3)
            with open(self.output_filename, "a") as fh:
                fh.write(f"{start_time},{response_time},{status_code}\n")

    class BondResponseTimeReporter(ResponseTimeReporter):
        def __init__(self, output_filename):
            super().__init__(output_filename)

        # TODO Support configuration of these by project and deployment tier.
        BOND_HOST = "broad-bond-dev.appspot.com"
        BOND_PROVIDER = "fence"  # BDCat

        # When run in Terra, this returns the Terra user pet SA token
        @staticmethod
        def get_terra_user_pet_sa_token() -> str:
            import google.auth.transport.requests
            creds, projects = google.auth.default()
            creds.refresh(google.auth.transport.requests.Request())
            token = creds.token
            return token

        def measure_response_time(self) -> Tuple[float, int]:
            # Measure Bond response time
            terra_user_token = self.get_terra_user_pet_sa_token()
            print(f"{datetime.now()} checking bond response time on {currentThread().name}")
            start_time = time.time()
            headers = {
                'authorization': f"Bearer {terra_user_token}",
                'content-type': "application/json"
            }
            resp = requests.get(f"https://{self.BOND_HOST}/api/link/v1/{self.BOND_PROVIDER}/accesstoken",
                                headers=headers)
            duration = time.time() - start_time
            return duration, resp.status_code

    class FenceResponseTimeReporter(ResponseTimeReporter):
        def __init__(self, output_filename):
            super().__init__(output_filename)

        # TODO Support configuration of this by project and deployment tier.
        FENCE_HOST = "staging.gen3.biodatacatalyst.nhlbi.nih.gov"

        def measure_response_time(self) -> Tuple[float, int]:
            # Measure Fence health status check response time
            print(f"{datetime.now()} checking fence response time on {currentThread().name}")
            start_time = time.time()
            headers = {
                'accept': "*/*"
            }
            resp = requests.get(f"https://{self.FENCE_HOST}/_status", headers=headers)
            duration = time.time() - start_time
            return duration, resp.status_code


    @catch_exceptions()
    def check_bond_response_time(self):
        output_filename = "bond_fence_token_response_times.csv"
        reporter = self.BondResponseTimeReporter(output_filename)
        reporter.measure_and_report()
        
    @catch_exceptions()
    def check_fence_response_time(self):
        output_filename = "fence_health_status_response_times.csv"
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

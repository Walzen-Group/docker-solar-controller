import logging
import datetime
from threading import Thread, Event
import time
from typing import Callable

class TimedCalls(Thread):
    """Call function again every `interval` time duration after it's first run."""
    def __init__(self, func: Callable, interval: datetime.timedelta, start_offset_seconds: int = 0) -> None:
        super().__init__()
        self.func = func
        self.interval = interval
        self.stopped = Event()
        self.start_offset_seconds = start_offset_seconds

    def cancel(self):
        self.stopped.set()

    def wait_until_start_time(self, start_time):
        if datetime.datetime.now() >= start_time:
            return
        while datetime.datetime.now() < start_time:
            time.sleep(1)

    def run(self):
        self.func()
        now = datetime.datetime.now()
        start_time = self.get_nearest_interval_time(now, self.interval)
        if (offset_negative := start_time - datetime.timedelta(seconds=self.start_offset_seconds)) > now:
            start_time = offset_negative
        else:
            start_time = start_time + datetime.timedelta(seconds=self.start_offset_seconds)
        logging.info(f"waiting until {start_time} to start loop...")
        self.wait_until_start_time(start_time)
        next_call = time.time()
        logging.info(f"starting loop...")
        while not self.stopped.is_set():
            self.func()  # Target activity.
            next_call = next_call + self.interval.seconds
            # Block until beginning of next interval (unless canceled).
            self.stopped.wait(next_call - time.time())

    def get_nearest_interval_time(self, base_time, interval):
        base_time = base_time.replace(microsecond=0)
        seconds_since_midnight = int(time.time())
        interval_seconds = interval.total_seconds()
        remainder = seconds_since_midnight % interval_seconds
        if remainder == 0:
            return base_time
        else:
            delta = interval_seconds - remainder
            return base_time + datetime.timedelta(seconds=delta)

def query_influx(secrets, influx_client):
    query_api = influx_client.query_api()
    bucket = secrets['bucket']

    power_consumed_query = f'from(bucket:"{bucket}")\
        |> range(start: -15m)\
        |> filter(fn: (r) => r._measurement == "fusionsolarpy")\
        |> filter(fn: (r) => r.component == "power")\
        |> filter(fn: (r) => r._field == "power_consumed")'

    power_produced_query = f'from(bucket:"{bucket}")\
        |> range(start: -15m)\
        |> filter(fn: (r) => r._measurement == "fusionsolarpy")\
        |> filter(fn: (r) => r.component == "power")\
        |> filter(fn: (r) => r._field == "power_produced")'

    server_power_draw_query = f'from(bucket:"{bucket}")\
        |> range(start: -1m)\
        |> filter(fn: (r) => r._measurement == "tinytuya")\
        |> filter(fn: (r) => r.socket == "server")\
        |> filter(fn: (r) => r._field == "power")\
        |> last()'
    try:
        power_consumed_result = query_api.query(org=secrets['org'], query=power_consumed_query)
        power_produced_result = query_api.query(org=secrets['org'], query=power_produced_query)
        server_power_result = query_api.query(org=secrets['org'], query=server_power_draw_query)
    except Exception as e:
        logging.warning(f"failed to query influx: {e}")
        return None, None

    try:
        server_power = next(record for record in next(table for table in server_power_result))
        power_produced = []
        power_consumed = []

        for table in power_produced_result:
            for record in table.records:
                power_produced.append(record)

        for table in power_consumed_result:
            for record in table.records:
                power_consumed.append(record)

        current_values = {
            "server_power": server_power,
            "power_produced": power_produced[-1],
            "power_consumed": power_consumed[-1],
            "query_time": power_consumed[-1].get_time().astimezone()
        }
        previous_values = {
            "power_produced": power_produced[-2],
            "power_consumed": power_consumed[-2],
            "query_time": power_consumed[-2].get_time().astimezone()
        }
        return previous_values, current_values

    except StopIteration as e:
        logging.warning(f"No data returned from influx: {e}")

    return None, None


def query_docker(docker_client, container_name):
    container = docker_client.container.inspect(container_name)
    container_running = container.state.running
    container_paused = container.state.paused

    return container_running, container_paused


def get_tdarr_node_running_status(session, config):
    r = session.get(f'{config["tdarrUrl"]}/api/v2/get-nodes')
    r.raise_for_status()
    json = r.json()
    if len(json.keys()) > 0:
        key = list(json.keys())[0]
    else:
        raise Exception("no nodes found")
    return json[key]['nodePaused'], key


def set_tdarr_node_status(session, node_id, pause, config):
    r = session.post(f'{config["tdarrUrl"]}/api/v2/update-node', json={
        "data": {
            "nodeID": node_id,
            "nodeUpdates": {
                "nodePaused": pause
            }
        }
    })
    r.raise_for_status()


def update_tdarr_node(session, node_id, config, pause):
    try:
        set_tdarr_node_status(session, node_id, pause, config)
    except Exception as e:
        logging.warning(f"failed to update tdarr node status: {e}")


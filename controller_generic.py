import json
import logging
from time import sleep
from python_on_whales import docker as docker_client
from influxdb_client import InfluxDBClient

logging.basicConfig(
    format='| %(levelname)s | %(asctime)s | %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("controller.log"),
        logging.StreamHandler()
])

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

    power_consumed_result = query_api.query(org=secrets['org'], query=power_consumed_query)
    power_produced_result = query_api.query(org=secrets['org'], query=power_produced_query)
    server_power_result = query_api.query(org=secrets['org'], query=server_power_draw_query)

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
        }
        previous_values = {
            "power_produced": power_produced[-2],
            "power_consumed": power_consumed[-2],
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


# parse secrets file as json
with open('secrets/controller_config.json') as f:
    config = json.load(f)


offline_server_power_estimate = config['baseWattsServer']
required_headroom_estimate = config['marginWatts']

influx_client = InfluxDBClient(url=config['influxUrl'], token=config['influxToken'])
#docker_client = docker = DockerClient(host="ssh://UnraidTemp")

container_name = 'tdarr'

logging.info(f"starting loop with monitored container {container_name}...")

while True:
    container_running, container_paused = query_docker(docker_client, container_name)
    previous_values, current_values = query_influx(config, influx_client)
    if not previous_values or not current_values:
        logging.warning("no data returned from influx, sleeping for 60 seconds")
        if container_running and not container_paused:
            logging.info(f"pausing {container_name}]")
            docker_client.container.pause(container_name)
        sleep(60)
        continue

    headroom_now = max(round((current_values['power_produced'].get_value() - current_values['power_consumed'].get_value()) * 1000, 2), 0)
    headroom_previous = max(round((current_values['power_produced'].get_value() - previous_values['power_consumed'].get_value()) * 1000, 2), 0)
    current_server_power = current_values['server_power'].get_value()

    if container_running:
        if headroom_now == headroom_previous:
            logging.info(f"headroom steady at: {headroom_now}W")
        elif headroom_now > headroom_previous:
            logging.info(f"headroom rising: now: {headroom_now}W, previous: {headroom_previous}W, change: {headroom_now - headroom_previous}W")
        else:
            logging.info(f"headroom falling: now: {headroom_now}W, previous: {headroom_previous}W, change: {headroom_now - headroom_previous}W")

        if container_paused:
            # estimate how much headroom would be required to stay green, cap at min margin

            # last two readings have to be above the required margin
            if headroom_now >= required_headroom_estimate and headroom_previous >= required_headroom_estimate:
                logging.info(f"unpausing {container_name}")
                logging.info(f"required margin: {required_headroom_estimate}W")
                logging.info(f"current server power draw: {current_server_power}W")
                logging.info(f"current solar power headroom: {headroom_now}W")
                logging.info(f"last solar power headroom: {headroom_previous}W")
                docker_client.container.unpause(container_name)
        elif not container_paused:
            required_headroom_estimate = round(max(current_server_power - offline_server_power_estimate, config['marginWatts']), 2)

            if headroom_now < required_headroom_estimate:
                logging.info(f"pausing {container_name}")
                logging.info(f"required margin for operation: {required_headroom_estimate}W")
                logging.info(f"current server power draw: {current_values['server_power']}W")
                logging.info(f"current solar power headroom: {headroom_now}W")
                docker_client.container.pause(container_name)

    if not container_running or container_paused:
        offline_server_power_estimate = current_server_power

    sleep(60)


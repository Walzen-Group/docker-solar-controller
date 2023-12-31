import json
import logging
import requests
from time import sleep
from python_on_whales import docker
from python_on_whales import DockerClient
from influxdb_client import InfluxDBClient
from lib import *

logging.basicConfig(
    format='| %(levelname)s | %(asctime)s | %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("controller_tdarr_api.log"),
        logging.StreamHandler()
])


class Controller:
    def __init__(self, container_name):
        # parse secrets file as json
        with open('secrets/controller_config.json') as f:
            self.config = json.load(f)

        self.session = requests.Session()

        self.offline_server_power_estimate = self.config['baseWattsServer']
        self.required_headroom_estimate = self.config['marginWatts']
        self.container_name = container_name
        self.query_interval = self.config['queryIntervalSeconds']

        self.influx_client = InfluxDBClient(url=self.config['influxUrl'], token=self.config['influxToken'])
        #self.docker_client = DockerClient(host="ssh://UnraidTemp")
        self.docker_client = docker

    def run_loop(self):
        container_running, _ = query_docker(self.docker_client, self.container_name)
        plex_running, plex_paused = query_docker(self.docker_client, "PlexMediaServer")


        if container_running:
            try:
                node_paused, node_id = get_tdarr_node_running_status(self.session, self.config)
            except Exception as e:
                logging.warning(f"failed to get tdarr node status: {e}, retrying in 120 seconds")
                sleep(120)
                return

        previous_values, current_values = query_influx(self.config, self.influx_client)
        if not previous_values or not current_values:
            logging.warning(f"no data returned from influx, sleeping for {self.query_interval} seconds")
            if container_running and not node_paused:
                logging.info(f"pausing {self.container_name}]")
                update_tdarr_node(self.session, node_id, self.config, pause=True)
                if plex_running and plex_paused:
                    self.docker_client.container.unpause("PlexMediaServer")
            sleep(self.query_interval)
            return

        headroom_now = int(max(round((current_values['power_produced'].get_value() -
                        current_values['power_consumed'].get_value()) * 1000, 2), 0))
        headroom_previous = int(max(round((previous_values['power_produced'].get_value(
        ) - previous_values['power_consumed'].get_value()) * 1000, 2), 0))
        current_server_power = current_values['server_power'].get_value()

        if container_running:
            if headroom_now == headroom_previous:
                if headroom_now != 0:
                    logging.info(f"headroom steady at: {headroom_now}W")
            elif headroom_now > headroom_previous:
                logging.info(
                    f"headroom rising:   change: {(headroom_now - headroom_previous):>+5}W, values: {headroom_now:>4}W @ {current_values['query_time']:%H:%M:%S}, {headroom_previous:>4}W @ {previous_values['query_time']:%H:%M:%S}")
            else:
                logging.info(
                    f"headroom falling:  change: {(headroom_now - headroom_previous):>+5}W, values: {headroom_now:>4}W @ {current_values['query_time']:%H:%M:%S}, {headroom_previous:>4}W @ {previous_values['query_time']:%H:%M:%S}")

            if node_paused:
                # last two readings have to be above the required margin
                if headroom_now >= self.required_headroom_estimate and headroom_previous >= self.required_headroom_estimate:
                    logging.info(f"unpausing {self.container_name}")
                    logging.info(f"required margin: {self.required_headroom_estimate}W")
                    logging.info(f"current server power draw: {current_server_power}W")
                    logging.info(f"current solar power headroom: {headroom_now}W")
                    logging.info(f"last solar power headroom: {headroom_previous}W")
                    update_tdarr_node(self.session, node_id, self.config, pause=False)
                    if plex_running and not plex_paused:
                        self.docker_client.container.pause("PlexMediaServer")

            elif not node_paused:
                # estimate how much headroom would be required to stay green, cap at min margin
                self.required_headroom_estimate = round(
                    max(current_server_power - self.offline_server_power_estimate, self.config['marginWatts']), 2)

                # if no more headroom is left we need to stahp
                if headroom_now < 10:
                    logging.info(f"pausing {self.container_name}")
                    logging.info(f"required margin for operation: {self.required_headroom_estimate}W")
                    logging.info(f"current server power draw: {current_server_power}W")
                    logging.info(f"current solar power headroom: {headroom_now}W")
                    update_tdarr_node(self.session, node_id, self.config, pause=True)
                    if plex_running and plex_paused:
                        self.docker_client.container.unpause("PlexMediaServer")


        if not container_running or node_paused:
            self.offline_server_power_estimate = current_server_power

    def start(self, query_offset_seconds: int = 0):
        logging.info(f"starting controller, {self.query_interval}s query interval with monitored container {self.container_name}...")
        self.loop = TimedCalls(self.run_loop, datetime.timedelta(seconds=self.query_interval), query_offset_seconds)
        self.loop.start()

    def stop(self):
        logging.info(f"stopping controller")
        self.loop.cancel()



Controller('tdarr').start(40)

import logging, json
from threading import Thread
from config.config import Config
from TT_agent import TT_agent
import datetime, time, traceback
from protocols.amqp.send import Sender



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_capability(url, topic, period, capability_data):
    while True:
        # Publish Capability in "/capabilities"
        try:
            capability_sender.send(url, topic, capability_data)
            logging.info('Capability sent')
        except Exception as e:
            logging.error("Agent can't send capability to the controller. Traceback:")
            logging.error(traceback.format_exc())
        time.sleep(period)

def start_TT_agent():
    # Load configuration
    cfg = Config()

    storage_agent = TT_agent(config=cfg)

    with open("capability/photon_counting_measurement.json", 'r') as capability_file:
        capability_data = json.load(capability_file)

    capability_data["timestamp"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
    topic = 'topic://'+'/capabilities'
    thread_send_capability = Thread(target=send_capability, args=(cfg.amqp_broker, topic, cfg.capability_period, capability_data), daemon=True)
    endpoint = capability_data['endpoint']
    #thread_send_capability.start()

    # Subscribe the ControllerService to events and pass the StorageAgent
    storage_agent.subscribe_to_telemetry_service(endpoint=endpoint)

if __name__ == '__main__':
    # Start the storage agent
    capability_sender = Sender()
    start_TT_agent()

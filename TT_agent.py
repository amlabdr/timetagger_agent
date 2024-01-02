import logging, json, re, time
from protocols.amqp.receive import Receiver
from protocols.amqp.send import Sender
from datetime import datetime
from threading import Thread, Event

class TT_agent:
    def __init__(self, config) -> None:
        self.cfg = config
        self.sender = Sender()
        self.running_specs = {}  # Dictionary to store running specifications and their threads

    def when_to_time(self, when):
        match = re.search(r'(now|\d+)\s*\.\.\.\s*(\d+)\s*/\s*(\d+)', when)
        if match:
            start_time = match.group(1)
            if start_time == "now":
                start_time = int(datetime.now().timestamp())
            else:
                start_time = int(start_time)
            stop_time = int(match.group(2))
            period = int(match.group(3))
        else:
            raise ValueError("Invalid time string format")

        return start_time, stop_time, period

    def subscribe_to_telemetry_service(self,endpoint):
        topic='topic://'+endpoint+'/specifications'
        url = self.cfg.amqp_broker
        logging.info("Agent will start lesstning for events from the telemetry service")
        receiver = Receiver(on_message_callback=self.telemetry_service_on_message_callback)
        receiver.receive_event(url,topic)
    
    def send_receipt(self, event):
        receipt_msg=json.loads(event.message.body)
        receipt_msg['receipt'] = receipt_msg['specification']
        receipt_msg["timestamp"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
        ##########
        receipt_msg["result_topic"] = '/'+receipt_msg['endpoint']+'/results'
        del receipt_msg['specification']
        topic_reply_to =  event.message.reply_to
        url = self.cfg.amqp_broker
        self.sender.send(url, topic = topic_reply_to, messages= receipt_msg)

    def get_data(self):
        return {"WR_time":datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4], "timetags":[1,2,3,4]}
    
    def process_specification(self, specification_msg, interrupt_event):
        endpoint = specification_msg['endpoint']
        when = specification_msg['when']
        start_time , stop_time, period = self.when_to_time(when)
        # Simulate running the specification
        for timestamp in range(start_time, stop_time, period):
            if interrupt_event.is_set():
                logging.info(f"Specification {specification_msg['specification']} interrupted.")
                spec_name = specification_msg['specification']
                unique_id = specification_msg['schema'] 
                key_to_delete = (spec_name, unique_id)
                if key_to_delete in self.running_specs:
                    del self.running_specs[key_to_delete]
                return
            logging.info(f"Running specification {specification_msg['specification']} at timestamp {timestamp}")
            result_msg = specification_msg.copy()
            result_msg['result'] = result_msg['specification']
            del result_msg['specification']
            resultValues= []
            resultValues.append(self.get_data())
            result_msg["timestamp"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
            result_msg['resultValues'] = [resultValues]
            result_topic = 'topic://'+endpoint+'/results'
            url = self.cfg.amqp_broker
            print("result_msg:",result_msg)
            self.sender.send(url,result_topic, result_msg)
            time.sleep(5)

    
    def telemetry_service_on_message_callback(self, event):
        specification_msg = json.loads(event.message.body)
        logging.info("msg received {}".format(specification_msg))
        if'specification' in specification_msg:
            self.send_receipt(event)
            spec_name = specification_msg['specification']
            unique_id = specification_msg['schema'] 
            interrupt_event = Event()
            thread = Thread(target=self.process_specification, args=(specification_msg, interrupt_event))
            self.running_specs[(spec_name, unique_id)] = (thread, interrupt_event)
            thread.start()
        elif 'interrupt' in specification_msg:
            spec_name = specification_msg['interrupt']
            unique_id = specification_msg['schema']
            key = (spec_name, unique_id)
            if key in self.running_specs:
                _, interrupt_event = self.running_specs[key]
                interrupt_event.set()
                logging.info(f"Interrupt signal sent for specification {spec_name}")
            else:
                logging.warning(f"Specification {spec_name} not found.")
        else:
            logging.warning("Unknown message type.")
        
        

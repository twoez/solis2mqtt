import paho.mqtt.client as mqtt_client
import time
import logging

class Mqtt(mqtt_client.Client):
    def __init__(self, client_id, cfg):
        super().__init__(client_id=client_id, clean_session=True)
        self.enable_logger()
        self.username_pw_set(cfg['user'], cfg['passwd'])
        if cfg['use_ssl']:
            self.tls_set()
        if cfg['use_ssl'] and not cfg['validate_cert']:
            self.tls_insecure_set(True)
        self.on_connect = self._on_connect_callback
        
        # Store configuration for reconnection attempts
        self.cfg = cfg
        
        # Try to connect with retry logic
        self._connect_with_retry(cfg['url'], cfg['port'])
        
        self.loop_start()
        self.subscriptions = []

    def __del__(self):
        self.disconnect()

    def _on_connect_callback(self, client, userdata, flags, rc):
        if len(self.subscriptions):
            self.subscribe(self.subscriptions)
    
    def _connect_with_retry(self, url, port, max_retries=0, retry_interval=30):
        """
        Attempts to connect to the MQTT broker with retry logic
        
        Parameters:
        - url: MQTT broker URL
        - port: MQTT broker port
        - max_retries: Maximum number of retry attempts (0 = unlimited)
        - retry_interval: Time in seconds between retry attempts
        """
        retry_count = 0
        connected = False
        
        while not connected:
            try:
                logging.info(f"Attempting to connect to MQTT broker at {url}:{port}")
                self.connect(url, port)
                connected = True
                logging.info("Successfully connected to MQTT broker")
            except OSError as e:
                retry_count += 1
                if max_retries > 0 and retry_count >= max_retries:
                    logging.error(f"Failed to connect after {retry_count} attempts: {str(e)}")
                    raise  # Re-raise the exception if max retries reached
                
                logging.warning(f"Network error connecting to MQTT broker: {str(e)}")
                logging.info(f"Retrying in {retry_interval} seconds... (attempt {retry_count})")
                time.sleep(retry_interval)

    def persistent_subscribe(self, topic):
        self.subscriptions.append((topic, 0))
        self.subscribe(topic)
from confluent_kafka import Producer

class KafkaProducer:
    def __init__(self, bootstrap_servers, topic):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'delivery.timeout.ms': '0',  #Configuração para tempo limite de entrega
            'acks': 'all',                #Configuração de confirmação de recebimento
            'enable.idempotence': 'true'  #Ativa a idempotência
        })
        self.topic = topic
        
    def delivery_report(self, err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
            
    def send_message(self, message):
        self.producer.produce(self.topic, message.encode('utf-8'), callback=self.delivery_report)
        self.producer.poll(0)
        
    def flush(self):
        self.producer.flush()
        
def main():
    producer = KafkaProducer(bootstrap_servers='kafka_project-kafka-1:9092', topic='teste_messages')
    
    for i in range(10):
        message = f"message {i}"
        producer.send_message(message)
        
    producer.flush()
    
if __name__ == "__main__":
    main()
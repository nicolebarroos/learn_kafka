from confluent_kafka import Consumer, KafkaError

class KafkaConsumer:
    def __init__(self, bootstrap_servers, group_id, topic, client_id):
        print("Starting consumer...")
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'client.id': client_id,
            'auto.offset.reset': 'earliest'
        })
        self.topic = topic
        self.consumer.subscribe([self.topic])

    def consume_messages(self):
        while True:
            msg = self.consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            print('Received message: {}'.format(msg.value().decode('utf-8')))

    def close(self):
        self.consumer.close()

def main():
    consumer = KafkaConsumer(bootstrap_servers='kafka_project-kafka-1:9092', group_id='group2', topic='teste_messages', client_id="pyapp_consumer2")
    try:
        consumer.consume_messages()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    main()

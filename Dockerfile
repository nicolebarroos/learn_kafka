FROM python:3.10

RUN pip install confluent-kafka

WORKDIR /kafka_project

COPY producer.py consumer.py ./

CMD ["tail", "-f", "/dev/null"]
import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'kafka-service',
  brokers: ['localhost:9094'],
  connectionTimeout: 10000,
  requestTimeout: 10000,
});

const admin = kafka.admin();

const run = async () => {
  try {
    await admin.connect();
    await admin.createTopics({
      topics: [
        { topic: 'payment-successful', numPartitions: 1, replicationFactor: 1 },
        { topic: 'order-successful', numPartitions: 1, replicationFactor: 1 },
      ],
    });
    console.log('Topics created successfully');
  } catch (error) {
    console.error('Error creating topics:', error);
  } finally {
    await admin.disconnect();
  }
};

run();
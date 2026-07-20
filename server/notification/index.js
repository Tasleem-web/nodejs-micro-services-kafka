import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: ['localhost:9094', 'localhost:9095', 'localhost:9096'],
  connectionTimeout: 10000,
  requestTimeout: 10000,
});

const consumer = kafka.consumer({ groupId: 'notification-service' });
const producer = kafka.producer();

const run = async () => {
  try {
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({
      topic: 'email-successful',
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        const notificationEvent = JSON.parse(value);

        const { userId, emailId, cart } = notificationEvent;
        const notificationMessage = `Notification for user ${userId}: email ${emailId} sent successfully.`;

        console.log('Notification service received:', notificationEvent);
        console.log(notificationMessage);

        await producer.send({
          topic: 'notification-successful',
          messages: [
            {
              value: JSON.stringify({
                userId,
                emailId,
                cart,
                message: notificationMessage,
                deliveredAt: new Date().toISOString(),
              }),
            },
          ],
        });

        console.log('Published notification-successful event');
      },
    });
  } catch (error) {
    console.error('Error in notification-service:', error);
    process.exit(1);
  }
};

run();

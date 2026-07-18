import { Kafka } from 'kafkajs';


const kafka = new Kafka({
  clientId: 'order-service',
  brokers: ['localhost:9094'],
  connectionTimeout: 10000,
  requestTimeout: 10000,
});

const consumer = kafka.consumer({ groupId: "order-service" });
const producer = kafka.producer();

const run = async () => {
  try {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({
      topic: "payment-successful",
      fromBeginning: true
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        const { userId, cart } = JSON.parse(value);

        // TODO: Save order to database
        const dummyUserId = "abc123";

        console.log(`Order consume User: ${userId} and cart: ${JSON.stringify(cart)}`);

        await producer.send({
          topic: "order-successful",
          messages: [
            {
              value: JSON.stringify({
                userId,
                orderId: dummyUserId,
                cart,
              }),
            },
          ],
        });

      }
    });
  } catch (error) {
    console.log("Error in kafka", error);
  }
};

run();


import { Kafka } from 'kafkajs';


const kafka = new Kafka({
  clientId: 'email-service',
  brokers: ['localhost:9094', 'localhost:9095', 'localhost:9096'],
  connectionTimeout: 10000,
  requestTimeout: 10000,
});

const consumer = kafka.consumer({ groupId: "email-service" });
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
        const { userId, orderId, cart } = JSON.parse(value);

        // TODO: Send email to user
        const dummyEmailId = "emailIdabc123";

        console.log(`Email consume User: ${userId} and orderId: ${orderId} and cart: ${JSON.stringify(cart)}`);

        await producer.send({
          topic: "email-successful",
          messages: [
            {
              value: JSON.stringify({
                userId,
                emailId: dummyEmailId,
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


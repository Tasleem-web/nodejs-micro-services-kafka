import { Kafka } from 'kafkajs';


const kafka = new Kafka({
  clientId: 'analytics-service',
  brokers: ['localhost:9094'],
  connectionTimeout: 10000,
  requestTimeout: 10000,
});

const consumer = kafka.consumer({ groupId: "analytics-service" });


const run = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({
      topics: ["payment-successful", "order-successful", "email-successful"],
      fromBeginning: true
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        switch (topic) {
          case "payment-successful":
            const value = message.value.toString();
            const { userId, cart } = JSON.parse(value);
            const totalNumber = cart.reduce((acc, item) => acc + (+item.price * item.quantity), 0).toFixed(2);
            console.log(`Analytics consume User: ${userId} and paid: ${totalNumber}`);
            break;
          case "order-successful":
            const orderValue = message.value.toString();
            const { userId: orderUserId, orderId, cart: orderCart } = JSON.parse(orderValue);
            console.log(`Analytics consume: Order id ${orderId} created for User id: ${orderUserId}`);
            break;
          case "email-successful":
            const emailValue = message.value.toString();
            const { userId: emailUserId, emailId, cart: emailCart } = JSON.parse(emailValue);
            console.log(`Analytics consume: Email id: ${emailId} sent to User id: ${emailUserId}`);
            break;
        }
      }
    });
  } catch (error) {
    console.log("Error in kafka", error);
  }

};

run();


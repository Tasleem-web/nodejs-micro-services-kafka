import express from 'express';
import cors from 'cors';
import { Kafka } from 'kafkajs';

const PORT = process.env.PORT || 3000;

const app = express();

app.use(express.json());
app.use(cors());

const kafka = new Kafka({
  clientId: 'kafka-service',
  brokers: ['localhost:9094'],
  connectionTimeout: 10000,
  requestTimeout: 10000,
});

const producer = kafka.producer();

const connectToKafka = async () => {
  try {
    await producer.connect();
    console.log("Producer is running...");
  } catch (error) {
    console.log("Error continuing in kafka", error);
  }
};

app.post('/payment-service', async (req, res) => {

  const { cart } = req.body;
  console.log("Payment service is running...", cart);

  const userId = "123"

  // TODO: Implement payment service logic here, such as processing payments, handling transactions, etc.
  console.log(`Processing payment for user: ${userId}`);

  // TODO: KAFKA: Implement Kafka producer logic here to send payment events to the Kafka topic.
  await producer.send({
    topic: "payment-successful",
    messages: [
      {
        value: JSON.stringify({ userId, cart })
      }
    ]
  });

  res.send('Payment successful!');
});

app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).send('Something went wrong!');
});


app.listen(PORT, () => {
  connectToKafka();
  console.log(`Payment service is running on port => http://localhost:${PORT}`);
});
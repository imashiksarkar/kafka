process.removeAllListeners("warning");
process.on("warning", () => undefined);

import { Kafka } from "kafkajs";

async function run() {
  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"],
  });

  const producer = kafka.producer();
  await producer.connect();

  const topicName = "order.paid";

  await producer.send({
    topic: topicName,
    messages: [
      {
        key: "mac4",
        value: JSON.stringify({
          type: "order.paid",
          id: "123",
          status: "paid",
          paidAt: new Date(),
          order: { id: "123", amount: 100, currency: "USD" },
        }),
        headers: {
          type: "electronic",
        },
      },
    ],
  });
  console.log("🎉 Message sent successfully.");

  await producer.disconnect();
  console.log("🔌 Disconnected from Kafka.");
}

run().catch(console.error);

process.removeAllListeners("warning");
process.on("warning", () => undefined);

import { Kafka } from "kafkajs";

async function run() {
  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"],
  });

  const consumer = kafka.consumer({ groupId: "test-group" });
  await consumer.connect();

  const topicName = "order.paid";

  await consumer.subscribe({ topic: topicName, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message, topic, partition }) => {
      if (!message.value) return;

      const body = JSON.parse(message.value.toString());
      console.log({ body });

      console.log({ type: message.headers?.type?.toString() });

      const headers: Record<string, any> = {};
      if (message.headers) {
        for (const key in message.headers) {
          const headerValue = message.headers[key];
          if (headerValue) {
            headers[key] = headerValue.toString();
          }
        }
      }

      console.log({ headers });

      console.log({ key: message.key?.toString(), topic, partition });
    },
  });

  // await consumer.disconnect();
  // console.log("🔌 Disconnected from Kafka.");
}

run().catch(console.error);

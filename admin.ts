process.removeAllListeners("warning");
process.on("warning", () => undefined);

import { Kafka } from "kafkajs";

async function run() {
  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"],
  });

  const admin = kafka.admin();
  await admin.connect();

  const topicName = "order.paid";

  const topics = await admin.listTopics();

  if (topics.includes(topicName)) {
    console.log(`ℹ️ Topic "${topicName}" already exists, skipping creation.`);
    await admin.disconnect();
    return;
  }

  await admin.createTopics({
    topics: [{ topic: topicName, numPartitions: 2, replicationFactor: 1 }],
  });
  console.log(`🎉 Topic "${topicName}" created successfully.`);

  await admin.disconnect();
  console.log("🔌 Disconnected from Kafka.");
}

run().catch(console.error);

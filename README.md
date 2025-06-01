# Durable Kafka Messaging (.NET)

This repository demonstrates using **Azure Durable Functions** to send messages to **Apache Kafka**, and consuming them using a **.NET Core Console App**.

## 🔧 Tech Stack

- Azure Durable Functions (.NET 6)
- Confluent.Kafka (Kafka .NET client)
- Apache Kafka (Docker)
- .NET 6 Console App

## 📁 Projects

| Project               | Description                            |
|-----------------------|----------------------------------------|
| `KafkaMessageProducer` | Durable Function to publish messages to Kafka |
| `KafkaMessageConsumer` | Console App to read messages from Kafka |

## 🐳 Running Kafka Locally

```bash
docker-compose up -d

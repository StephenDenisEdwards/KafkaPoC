# How can I get started with PoC type projects to demonstrate Kafka capabilities. I typically use .NET Core as my development platform.

Here's a structured, step-by-step approach to quickly and effectively create **Proof of Concept (PoC)** projects demonstrating Kafka capabilities using  **.NET Core** .

---

## ✅ Step 1: Environment Setup

### Option A: Local Kafka Cluster with Docker

The simplest and quickest way to get Kafka running locally:

* Use a Docker Compose file from the official Confluent Kafka quickstart:

```bash
curl --silent --output docker-compose.yml \
https://raw.githubusercontent.com/confluentinc/cp-all-in-one/7.6.0-post/cp-all-in-one/docker-compose.yml

docker-compose up -d
```

* This command starts a Kafka broker, Zookeeper, Schema Registry, and other Confluent tools in a single command.

### Option B: Managed Kafka Service (Cloud)

For a fully managed environment:

* [Confluent Cloud](https://www.confluent.io/confluent-cloud/)
* [AWS Managed Streaming for Kafka (MSK)](https://aws.amazon.com/msk/)
* [Azure Event Hubs](https://azure.microsoft.com/en-us/services/event-hubs/) (Kafka compatible)

---

## ✅ Step 2: .NET Core Project Setup

### 1. Create .NET Core Solution

```bash
dotnet new sln -n KafkaPoC
dotnet new console -n KafkaProducer
dotnet new console -n KafkaConsumer
dotnet sln add KafkaProducer/KafkaProducer.csproj
dotnet sln add KafkaConsumer/KafkaConsumer.csproj
```

### 2. Add Kafka Client Libraries

The most popular Kafka .NET client is from  **Confluent** :

```bash
cd KafkaProducer
dotnet add package Confluent.Kafka

cd ../KafkaConsumer
dotnet add package Confluent.Kafka
```

---

## ✅ Step 3: Kafka Producer in .NET

### Minimal Producer Example (`KafkaProducer/Program.cs`):

```csharp
using System;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program
{
    static async Task Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        Console.WriteLine("Kafka Producer started, sending messages...");
        for (int i = 0; i < 10; i++)
        {
            var value = $"Hello Kafka! Message {i} at {DateTime.UtcNow}";
            var result = await producer.ProduceAsync("test-topic", new Message<Null, string> { Value = value });

            Console.WriteLine($"Sent '{value}' to partition {result.Partition}, offset {result.Offset}");
            await Task.Delay(1000);
        }

        producer.Flush(TimeSpan.FromSeconds(5));
    }
}
```

* Adjust `"localhost:9092"` if your Kafka is running elsewhere.
* Replace `"test-topic"` with your desired Kafka topic name.

---

## ✅ Step 4: Kafka Consumer in .NET

### Minimal Consumer Example (`KafkaConsumer/Program.cs`):

```csharp
using System;
using System.Threading;
using Confluent.Kafka;

class Program
{
    static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "my-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe("test-topic");

        Console.WriteLine("Kafka Consumer started, waiting for messages...");

        var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent immediate process exit
            cts.Cancel();
        };

        try
        {
            while (!cts.Token.IsCancellationRequested)
            {
                var cr = consumer.Consume(cts.Token);
                Console.WriteLine($"Received message '{cr.Message.Value}' from partition {cr.Partition}, offset {cr.Offset}");
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            consumer.Close();
        }
    }
}
```

* `"my-group"` identifies the consumer group.
* Ensure topic name matches the producer's.

---

## ✅ Step 5: Run the PoC Locally

* First, create the topic (optional, Kafka auto-creates if enabled):

```bash
docker exec -it broker kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

* Run Producer and Consumer:

```bash
dotnet run --project KafkaProducer/KafkaProducer.csproj
dotnet run --project KafkaConsumer/KafkaConsumer.csproj
```

---

## ✅ Step 6: Extend PoC with Additional Kafka Capabilities

To demonstrate broader Kafka features, progressively extend your PoC by adding these:

### 1. **Schema Management & Avro Serialization**

* Integrate with [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) for Avro schemas.
* Use `Confluent.SchemaRegistry` & `Confluent.SchemaRegistry.Serdes.Avro` NuGet packages.

### 2. **Dead Letter Queue (DLQ)**

* Implement logic in consumer to handle and route invalid messages to a separate DLQ topic.

### 3. **Kafka Connect**

* Demonstrate data integration from/to databases or files using Kafka Connect.

### 4. **Kafka Streams (via ksqlDB or Kafka Streams .NET)**

* Demonstrate stream processing with [ksqlDB](https://ksqldb.io/) or [Kafka Streams .NET](https://github.com/LGouellec/kafka-streams-dotnet).

---

## ✅ Step 7: Monitoring and Visualization (Optional but Recommended)

* [Confluent Control Center](https://docs.confluent.io/platform/current/control-center/index.html): visual monitoring of Kafka cluster, messages, topics, and schema registry.
* Use Kafka UI tools like [Kafdrop](https://github.com/obsidiandynamics/kafdrop) or [Conduktor](https://www.conduktor.io/) for visualization and easier demonstration.

---

## ✅ Step 8: Documentation and Presentation

* Clearly document the objectives, architecture, and results of your PoC:
  * Objectives: "Demonstrate Kafka Pub/Sub, durable event storage, event replay capabilities."
  * Architecture Diagrams: Kafka producer → Kafka topics → Kafka consumer.
  * Results: Performance metrics, message throughput, reliability demonstrations.

---

## ✅ Recommendations & Best Practices for .NET Kafka PoC Projects

* **Use asynchronous APIs** (`ProduceAsync`) to enhance performance.
* Configure Kafka topics explicitly (partitions, retention) for clarity in your PoC.
* Clearly separate producer and consumer projects to demonstrate Kafka's decoupled architecture.
* Incorporate Schema Registry early to highlight best practice schema management.
* Showcase fault tolerance by stopping and restarting the consumer to demonstrate persistent message storage and offset management.
* Monitor your PoC environment using GUI tools to visually communicate Kafka's operational characteristics.

---

## 📌 **Summary Checklist**

| Step | Task                                  | Status |
| ---- | ------------------------------------- | ------ |
| 1    | Kafka Cluster setup (Docker or Cloud) | ✅     |
| 2    | .NET Solution & Confluent.Kafka setup | ✅     |
| 3    | Implement Kafka Producer              | ✅     |
| 4    | Implement Kafka Consumer              | ✅     |
| 5    | Run Basic Producer/Consumer           | ✅     |
| 6    | Extend with advanced Kafka features   | ⚙️   |
| 7    | Visualization & Monitoring            | ⚙️   |
| 8    | Document PoC & results                | ⚙️   |

---

Following this structured approach will allow you to quickly establish a robust Kafka PoC environment, effectively demonstrate Kafka’s capabilities, and set the stage for more advanced exploration and production-grade usage within your .NET Core development environment.


# Ports

When running Kafka locally in Docker (e.g., using Docker Compose), you'll typically use standard Kafka ports. Here's how to check which ports to use, and how to verify your Kafka and related services:

---

## ✅ **Default Kafka Ports**

Kafka and related services use these standard default ports:

| Service         | Default Port | Description                      |
| --------------- | ------------ | -------------------------------- |
| Kafka Broker    | **9092**     | Default Kafka client connections |
| Zookeeper       | **2181**     | Cluster coordination service     |
| Schema Registry | **8081**     | Schema Registry REST API         |
| Kafka Connect   | **8083**     | Kafka Connect REST API           |
| Control Center  | **9021**     | Confluent Control Center UI      |
| ksqlDB Server   | **8088**     | ksqlDB REST API                  |

If you're running **Confluent's Quickstart Docker Compose** (common scenario), these ports are typically exposed.

---

## ✅ **Step-by-Step: Checking Ports in Docker Compose**

If you're running Kafka locally using Docker Compose:

### 1. Check your Docker Compose file

View ports exposed by the containers:

```bash
cat docker-compose.yml | grep ports -A 2
```

Typical example from Confluent quickstart (`docker-compose.yml`):

```yaml
  broker:
    image: confluentinc/cp-server:7.6.0
    ports:
      - "9092:9092"

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    ports:
      - "8081:8081"
```

**Interpretation:**
`"9092:9092"` means Kafka broker port **9092** is exposed on your localhost at **9092**.

---

### 2. Check Running Docker Containers & Ports

Run the following command to see active containers and mapped ports:

```bash
docker ps
```

Output might look like:

```
CONTAINER ID   IMAGE                                     PORTS
a1b2c3d4e5f6   confluentinc/cp-server:7.6.0              0.0.0.0:9092->9092/tcp
b2c3d4e5f6a1   confluentinc/cp-schema-registry:7.6.0     0.0.0.0:8081->8081/tcp
```

* This confirms Kafka Broker on **9092**, Schema Registry on **8081** are accessible on your local machine.

---

## ✅ **Verify Ports (Quick Test Commands)**

### Check if Kafka is running:

```bash
nc -zv localhost 9092
```

* Expected output:

```
Connection to localhost port 9092 [tcp/*] succeeded!
```

### Check Schema Registry:

```bash
curl -X GET http://localhost:8081/subjects
```

* Expected output (JSON response):

```
[]
```

---

## ✅ **Check Kafka Ports with Docker CLI (Detailed Method)**

Inspect detailed Docker container configuration for ports:

```bash
docker inspect broker | grep HostPort
```

Output example:

```json
"HostPort": "9092"
```

This explicitly confirms the port Kafka broker is listening on locally.

---

## ✅ **Using Kafka Ports in your .NET Client Configurations**

In your .NET Producer/Consumer apps, use these local ports:

```csharp
var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
```

Similarly, Schema Registry (for Avro) configuration:

```csharp
var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://localhost:8081" };
```

---

## ✅ **Common Kafka Docker Compose Ports Summary**

| Kafka Component     | Docker-Exposed Port | Access from .NET Core apps                           |
| ------------------- | ------------------- | ---------------------------------------------------- |
| **Kafka Broker**    | **9092**            | `BootstrapServers = "localhost:9092"`                |
| **Schema Registry** | **8081**            | `Url = "http://localhost:8081"`                      |
| **Zookeeper**       | **2181**            | Internal to Kafka broker (usually no direct app use) |
| **Control Center**  | **9021**            | Browser: `http://localhost:9021`                     |
| **Kafka Connect**   | **8083**            | REST API: `http://localhost:8083`                    |

---

## 🎯 **Summary & Recommended Steps**

* ✅ Check Docker Compose file or run `docker ps` to see port mappings clearly.
* ✅ Confirm Kafka Broker (`9092`) and Schema Registry (`8081`) are accessible.
* ✅ Test using tools (`nc`, `curl`) to quickly verify connectivity.
* ✅ Configure .NET Core apps with these confirmed ports.

Following these steps ensures you reliably understand and manage ports for Kafka and its ecosystem running locally via Docker.



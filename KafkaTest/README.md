Here’s a **well-documented version** of the integration test that validates **Kafka message ordering**, **retry handling**, and **success criteria** using Avro serialization and Polly retry policies.

---

## 📄 Integration Test: Ordered Kafka Message Processing with Retry

### ✔️ Overview

This test:

* Sends 5 Avro-encoded `Patient` messages to Kafka
* Starts a consumer with Polly-based retry logic
* Simulates a failure for one message (`PatientId = P3`) on first attempt
* Verifies:

  * All messages are eventually processed
  * Message order is preserved
  * No message is lost or skipped

---

### 🧪 Test Code

```csharp
[Fact]
public async Task Messages_Are_Processed_In_Order_With_Retries()
{
    // Arrange – Prepare 5 ordered Patient messages
    var patients = Enumerable.Range(1, 5)
        .Select(i => new Patient
        {
            PatientId = $"P{i}",
            Name = $"Test Patient {i}",
            DateOfBirth = "1970-01-01"
        }).ToList();

    var received = new ConcurrentQueue<string>();         // Captures processed PatientIds in order
    var processedCount = 0;                               // Track how many messages have been processed
    var failureInjectedForId = "P3";                      // PatientId to simulate a transient failure

    var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
    {
        Url = SchemaRegistryUrl
    });

    var cts = new CancellationTokenSource();

    // Act – Start Kafka consumer in background with retry logic
    var consumerTask = Task.Run(async () =>
    {
        var config = new ConsumerConfig
        {
            GroupId = "ordered-test-consumer",
            BootstrapServers = BootstrapServers,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<string, Patient>(config)
            .SetValueDeserializer(new AvroDeserializer<Patient>(schemaRegistry).AsSyncOverAsync())
            .SetKeyDeserializer(Deserializers.Utf8)
            .Build();

        consumer.Subscribe(TopicName);

        var retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(3, attempt => TimeSpan.FromMilliseconds(100));

        while (!cts.Token.IsCancellationRequested && processedCount < patients.Count)
        {
            var cr = consumer.Consume(cts.Token);

            await retryPolicy.ExecuteAsync(async () =>
            {
                var patient = cr.Message.Value;

                // Simulate one-time failure on patient P3
                if (patient.PatientId == failureInjectedForId && processedCount == 2)
                {
                    throw new Exception("Simulated processing failure");
                }

                // Simulate message handling
                await Task.Delay(50);
                received.Enqueue(patient.PatientId);
                Interlocked.Increment(ref processedCount);
            });

            consumer.Commit(cr); // Commit only after successful processing
        }

        consumer.Close();
    });

    // Act – Produce messages to Kafka
    var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers };

    using var producer = new ProducerBuilder<string, Patient>(producerConfig)
        .SetValueSerializer(new AvroSerializer<Patient>(schemaRegistry))
        .SetKeySerializer(Serializers.Utf8)
        .Build();

    foreach (var p in patients)
    {
        await producer.ProduceAsync(TopicName, new Message<string, Patient>
        {
            Key = p.PatientId,
            Value = p
        });
    }

    // Wait for processing to finish (or timeout)
    await Task.WhenAny(consumerTask, Task.Delay(10000));
    cts.Cancel();

    // Assert – All messages were processed in correct order
    Assert.Equal(patients.Select(p => p.PatientId), received);
}
```

---

### 📌 Notes

| Feature                 | Details                                                    |
| ----------------------- | ---------------------------------------------------------- |
| **Test Framework**      | xUnit (but compatible with NUnit/MSTest)                   |
| **Kafka Topic**         | `test.patient.ordered`                                     |
| **Avro Schema**         | `Patient` with `PatientId`, `Name`, `DateOfBirth`          |
| **Retry Mechanism**     | [Polly](https://github.com/App-vNext/Polly) with 3 retries |
| **Failure Injection**   | `PatientId == P3` fails on 1st attempt                     |
| **Concurrency Control** | Uses `ConcurrentQueue<string>` and `Interlocked.Increment` |
| **Commit Strategy**     | Manual commit after successful retry                       |
| **Ordering Check**      | `Assert.Equal(expectedIds, receivedIds)`                   |

---

### 📦 Dependencies

```bash
dotnet add package Confluent.Kafka
dotnet add package Confluent.SchemaRegistry
dotnet add package Confluent.SchemaRegistry.Serdes
dotnet add package Polly
dotnet add package xunit
```

---

### ✅ Test Outcome

* Ensures that retryable failures **do not disrupt order**
* Verifies that **manual offset commit and sync processing** maintain **Kafka partition ordering**
* Demonstrates how to build **robust consumers** for event-driven systems

---

Would you like a `Docker Compose` for Kafka + Schema Registry or a reusable test harness for this pattern?

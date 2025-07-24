////namespace KafkaTest
////{
////	public class UnitTest1
////	{
////		[Fact]
////		public void Test1()
////		{

////		}
////	}
////}

//using System.Collections.Concurrent;
//using Confluent.Kafka;
//using Confluent.Kafka.SyncOverAsync;
//using Confluent.SchemaRegistry;
//using Confluent.SchemaRegistry.Serdes;
//using Example.PoC;
//using Polly;
//// Your generated Avro class namespace

//namespace KafkaTest;

//public class KafkaOrderAndRetryTest
//{
//	private const string BootstrapServers = "localhost:9092";
//	private const string SchemaRegistryUrl = "http://localhost:8081";
//	private const string TopicName = "test.patient.ordered";

//	[Fact]
//	public async Task Messages_Are_Processed_In_Order_With_Retries()
//	{
//		// Setup test data
//		var patients = Enumerable.Range(1, 5)
//			.Select(i => new Patient
//			{
//				PatientId = $"P{i}",
//				Name = $"Test Patient {i}",
//				DateOfBirth = "1970-01-01"
//			}).ToList();

//		// Shared collection for tracking received message order
//		var received = new ConcurrentQueue<string>();
//		var processedCount = 0;
//		var failureInjectedForId = "P3";

//		// Set up Schema Registry client
//		var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
//		{
//			Url = SchemaRegistryUrl
//		});

//		// Start consumer in background
//		var cts = new CancellationTokenSource();
//		var consumerTask = Task.Run(async () =>
//		{
//			var config = new ConsumerConfig
//			{
//				GroupId = "ordered-test-consumer",
//				BootstrapServers = BootstrapServers,
//				EnableAutoCommit = false,
//				AutoOffsetReset = AutoOffsetReset.Earliest
//			};

//			using var consumer = new ConsumerBuilder<string, Patient>(config)
//				.SetValueDeserializer(new AvroDeserializer<Patient>(schemaRegistry).AsSyncOverAsync())
//				.SetKeyDeserializer(Deserializers.Utf8)
//				.Build();

//			consumer.Subscribe(TopicName);

//			var retryPolicy = Policy
//				.Handle<Exception>()
//				.WaitAndRetryAsync(3, attempt => TimeSpan.FromMilliseconds(100));

//			while (!cts.Token.IsCancellationRequested && processedCount < patients.Count)
//			{
//				var cr = consumer.Consume(cts.Token);

//				await retryPolicy.ExecuteAsync(async () =>
//				{
//					var patient = cr.Message.Value;

//					// Simulate processing failure for one message
//					if (patient.PatientId == failureInjectedForId && processedCount == 2)
//					{
//						throw new Exception("Simulated processing failure");
//					}

//					// Simulate actual processing
//					await Task.Delay(50);
//					received.Enqueue(patient.PatientId);
//					Interlocked.Increment(ref processedCount);
//				});

//				consumer.Commit(cr);
//			}

//			consumer.Close();
//		});

//		// Start producer
//		var producerConfig = new ProducerConfig
//		{
//			BootstrapServers = BootstrapServers
//		};

//		using var producer = new ProducerBuilder<string, Patient>(producerConfig)
//			.SetValueSerializer(new AvroSerializer<Patient>(schemaRegistry))
//			.SetKeySerializer(Serializers.Utf8)
//			.Build();

//		foreach (var p in patients)
//		{
//			await producer.ProduceAsync(TopicName, new Message<string, Patient>
//			{
//				Key = p.PatientId,
//				Value = p
//			});
//		}

//		// Wait for consumption to complete
//		//await Task.WhenAny(consumerTask, Task.Delay(10000));
//		// Wait for all messages to be processed or timeout
//		var timeoutTask = Task.Delay(30000, cts.Token);

//		while (received.Count < patients.Count && !timeoutTask.IsCompleted)
//		{
//			await Task.Delay(100); // poll until all messages processed
//		}
//		cts.Cancel();

//		// Convert to list for deterministic assertion
//		var actualOrder = received.ToList();
//		var expectedOrder = patients.Select(p => p.PatientId).ToList();

//		Assert.Equal(expectedOrder, actualOrder);
//		// Final order verification
//		//Assert.Equal(patients.Select(p => p.PatientId), received);
//	}
//}
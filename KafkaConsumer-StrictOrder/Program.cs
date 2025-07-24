// Program.cs
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Example.PoC;   // your generated Patient class
using Polly;
using Polly.Retry;
using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.SyncOverAsync;

namespace AvroOrderedConsumer
{
	class Program
	{
		private const string BootstrapServers = "localhost:9092";
		private const string SchemaRegistryUrl = "localhost:8081";
		private const string Topic = "your-avro-topic";
		private const string DeadLetterTopic = "your-avro-topic.DLT";
		private const string GroupId = "avro-ordered-consumer-group";

		static async Task Main()
		{
			// 1) Ensure topics exist
			using var admin = new AdminClientBuilder(
				new AdminClientConfig { BootstrapServers = BootstrapServers }
			).Build();
			await EnsureTopicExistsAsync(admin, Topic, 1, 1);
			await EnsureTopicExistsAsync(admin, DeadLetterTopic, 1, 1);

			// 2) Start the producer in its own background Task
			var producerTask = StartProducerTask(Topic, messageCount: 50);

			// 3) Set up consumer + DLT producer + retry policy
			var registryConfig = new SchemaRegistryConfig { Url = SchemaRegistryUrl };
			var consumerConfig = new ConsumerConfig
			{
				BootstrapServers = BootstrapServers,
				GroupId = GroupId,
				AutoOffsetReset = AutoOffsetReset.Earliest,
				EnableAutoCommit = false,
				IsolationLevel = IsolationLevel.ReadCommitted
			};
			var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers };

			using var registry = new CachedSchemaRegistryClient(registryConfig);
			using var consumer = new ConsumerBuilder<Ignore, Patient>(consumerConfig)
									.SetValueDeserializer(new AvroDeserializer<Patient>(registry).AsSyncOverAsync())
									.Build();
			using var dlProducer = new ProducerBuilder<string, Patient>(producerConfig)
									.SetValueSerializer(new AvroSerializer<Patient>(registry))
									.Build();

			AsyncRetryPolicy retryPolicy = Policy
				.Handle<Exception>(IsRetryable)
				.WaitAndRetryAsync(
					retryCount: 3,
					sleepDurationProvider: i => TimeSpan.FromSeconds(Math.Pow(2, i)),
					onRetry: (ex, wait, i, ctx) =>
						Console.WriteLine($"[Retry {i}] waiting {wait:g}: {ex.Message}")
				);

			// 4) Consumer loop
			consumer.Subscribe(Topic);
			Console.CancelKeyPress += (_, e) =>
			{
				e.Cancel = true;
				consumer.Close();
			};

			Console.WriteLine("Consumer loop starting; producer is running in background...");
			while (true)
			{
				try
				{
					var cr = consumer.Consume(CancellationToken.None);
					await ProcessRecord(cr, retryPolicy, dlProducer, consumer);
				}
				catch (OperationCanceledException)
				{
					break;
				}
			}

			// Optionally wait for the producer to finish
			await producerTask;
		}

		/// <summary>
		/// Encapsulates all producer logic, runs on a background Task.
		/// </summary>
		private static Task StartProducerTask(string topic, int messageCount)
			=> Task.Run(async () =>
			{
				var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers };
				var registryConfig = new SchemaRegistryConfig { Url = SchemaRegistryUrl };

				using var registry = new CachedSchemaRegistryClient(registryConfig);
				using var producer = new ProducerBuilder<Null, Patient>(producerConfig)
					.SetValueSerializer(new AvroSerializer<Patient>(registry))
					.Build();

				Console.WriteLine($"[Producer] Sending {messageCount} Patient messages to '{topic}'...");
				for (int i = 1; i <= messageCount; i++)
				{
					var patient = new Patient
					{
						PatientId = i.ToString("D4"),
						Name = $"Patient {i}",
						DateOfBirth = DateTime.UtcNow.ToString("o")
					};

					var tp = new TopicPartition(topic, new Partition(0));
					await producer.ProduceAsync(tp,
						new Message<Null, Patient> { Value = patient });
					Console.WriteLine($"[Producer] Produced PatientId={patient.PatientId}");
					await Task.Delay(100); // simulate interval
				}

				producer.Flush(TimeSpan.FromSeconds(5));
				Console.WriteLine("[Producer] Done producing messages.");
			});

		private static async Task EnsureTopicExistsAsync(
			IAdminClient adminClient,
			string topicName,
			int partitions,
			short replicationFactor
		)
		{
			var spec = new TopicSpecification
			{
				Name = topicName,
				NumPartitions = partitions,
				ReplicationFactor = replicationFactor
			};

			try
			{
				Console.WriteLine($"[Admin] Creating topic '{topicName}'...");
				await adminClient.CreateTopicsAsync(new[] { spec });
				Console.WriteLine($"[Admin] Topic '{topicName}' created.");
			}
			catch (CreateTopicsException e)
			{
				var r = e.Results?[0];
				if (r != null && r.Error.Code == ErrorCode.TopicAlreadyExists)
					Console.WriteLine($"[Admin] Topic '{topicName}' already exists.");
				else
					throw;
			}
		}

		private static async Task ProcessRecord(
			ConsumeResult<Ignore, Patient> cr,
			AsyncRetryPolicy retryPolicy,
			IProducer<string, Patient> dlProducer,
			IConsumer<Ignore, Patient> consumer)
		{
			try
			{
				await retryPolicy.ExecuteAsync(() => HandlePatientAsync(cr.Message.Value));
				consumer.Commit(cr);
				Console.WriteLine($"[Consumer] Committed offset {cr.Offset.Value} (PatientId={cr.Message.Value.PatientId})");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[Consumer→DLT] Offset {cr.Offset.Value} → {ex.Message}");
				var dltMsg = new Message<string, Patient>
				{
					Key = cr.Message.Value.PatientId,
					Value = cr.Message.Value,
					Headers = cr.Message.Headers
				};
				await dlProducer.ProduceAsync(DeadLetterTopic, dltMsg);
				consumer.Commit(cr);
			}
		}

		private static Task HandlePatientAsync(Patient p)
		{
			Console.WriteLine($"[Handler] Processing PatientId={p.PatientId}, Name={p.Name}");
			return Task.CompletedTask;
		}

		private static bool IsRetryable(Exception ex)
			=> ex is TimeoutException || (ex is KafkaException ke && !ke.Error.IsFatal);
	}
}

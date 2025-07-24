// Program.cs
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Example.PoC;   // your generated Patient class
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
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
			var adminConfig = new AdminClientConfig { BootstrapServers = BootstrapServers };
			using var adminClient = new AdminClientBuilder(adminConfig).Build();

			await EnsureTopicExistsAsync(adminClient, Topic, partitions: 1, replicationFactor: 1);
			await EnsureTopicExistsAsync(adminClient, DeadLetterTopic, partitions: 1, replicationFactor: 1);

			// 2) Build Schema Registry, Consumer, and DLT Producer
			var schemaRegistryConfig = new SchemaRegistryConfig { Url = SchemaRegistryUrl };
			var consumerConfig = new ConsumerConfig
			{
				BootstrapServers = BootstrapServers,
				GroupId = GroupId,
				AutoOffsetReset = AutoOffsetReset.Earliest,
				EnableAutoCommit = false,
				IsolationLevel = IsolationLevel.ReadCommitted
			};
			var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers };

			using var registry = new CachedSchemaRegistryClient(schemaRegistryConfig);
			using var consumer = new ConsumerBuilder<Ignore, Patient>(consumerConfig)
				.SetValueDeserializer(new AvroDeserializer<Patient>(registry).AsSyncOverAsync())
				.Build();
			using var dlProducer = new ProducerBuilder<string, Patient>(producerConfig)
				.SetValueSerializer(new AvroSerializer<Patient>(registry))
				.Build();

			// 3) Polly retry policy (3 attempts, exponential backoff)
			AsyncRetryPolicy retryPolicy = Policy
				.Handle<Exception>(IsRetryable)
				.WaitAndRetryAsync(
					retryCount: 3,
					sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
					onRetry: (ex, wait, retryCount, ctx) =>
						Console.WriteLine($"[Retry {retryCount}] waiting {wait:g}: {ex.Message}")
				);

			// 4) Start consuming
			consumer.Subscribe(Topic);
			Console.CancelKeyPress += (_, e) =>
			{
				e.Cancel = true;
				consumer.Close();
			};

			Console.WriteLine("Starting Avro-ordered consumer loop...");
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
		}

		/// <summary>
		/// Creates the given topic if it does not already exist.
		/// </summary>
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
				Console.WriteLine($"Creating topic '{topicName}'...");
				await adminClient.CreateTopicsAsync(new[] { spec });
				Console.WriteLine($"Topic '{topicName}' created.");
			}
			catch (CreateTopicsException e)
			{
				var result = e.Results[0];
				if (result.Error.Code == ErrorCode.TopicAlreadyExists)
				{
					Console.WriteLine($"Topic '{topicName}' already exists; continuing.");
				}
				else
				{
					Console.WriteLine($"Error creating topic '{topicName}': {result.Error.Reason}");
					throw;
				}
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
				// Retryable business logic
				await retryPolicy.ExecuteAsync(() => HandlePatientAsync(cr.Message.Value));

				// Commit on success
				consumer.Commit(cr);
				Console.WriteLine($"[Commit] Offset {cr.Offset.Value} (PatientId={cr.Message.Value.PatientId})");
			}
			catch (Exception ex)
			{
				// Dead-letter on final failure
				Console.WriteLine($"[DLT] Offset {cr.Offset.Value} → {ex.Message}");
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
			// TODO: replace with real processing
			Console.WriteLine($"Processing PatientId={p.PatientId}, Name={p.Name}, DOB={p.DateOfBirth}");
			return Task.CompletedTask;
		}

		private static bool IsRetryable(Exception ex)
		{
			// e.g., network hiccups, transient Kafka exceptions, etc.
			if (ex is TimeoutException) return true;
			if (ex is KafkaException ke && !ke.Error.IsFatal) return true;
			return false;
		}
	}
}

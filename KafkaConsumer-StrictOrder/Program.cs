// Program.cs
using Confluent.Kafka;
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
			// 1) Configure Schema Registry + Consumer + DLT Producer
			var schemaRegistryConfig = new SchemaRegistryConfig { Url = SchemaRegistryUrl };
			var consumerConfig = new ConsumerConfig
			{
				BootstrapServers = BootstrapServers,
				GroupId = GroupId,
				AutoOffsetReset = AutoOffsetReset.Earliest,
				EnableAutoCommit = false,             // manual commit
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

			// 2) Build a Polly retry policy (3 tries, exponential backoff)
			AsyncRetryPolicy retryPolicy = Policy
				.Handle<Exception>(IsRetryable)
				.WaitAndRetryAsync(
					retryCount: 3,
					sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
					onRetry: (ex, wait, retryCount, ctx) =>
						Console.WriteLine($"[Retry {retryCount}] waiting {wait:g}: {ex.Message}")
				);

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

		private static async Task ProcessRecord(
			ConsumeResult<Ignore, Patient> cr,
			AsyncRetryPolicy retryPolicy,
			IProducer<string, Patient> dlProducer,
			IConsumer<Ignore, Patient> consumer)
		{
			try
			{
				// 3) Retryable business logic
				await retryPolicy.ExecuteAsync(() => HandlePatientAsync(cr.Message.Value));

				// 4) On success, commit offset
				consumer.Commit(cr);
				Console.WriteLine($"[Commit] Offset {cr.Offset.Value} (PatientId={cr.Message.Value.PatientId})");
			}
			catch (Exception ex)
			{
				// 5) Retries exhausted or non-retryable → dead-letter
				Console.WriteLine($"[DLT] Offset {cr.Offset.Value} → {ex.Message}");

				// Use PatientId as DLT key for traceability
				var dltMsg = new Message<string, Patient>
				{
					Key = cr.Message.Value.PatientId,
					Value = cr.Message.Value,
					Headers = cr.Message.Headers
				};
				await dlProducer.ProduceAsync(DeadLetterTopic, dltMsg);

				// Commit so we skip this bad record next time
				consumer.Commit(cr);
			}
		}

		private static Task HandlePatientAsync(Patient p)
		{
			// TODO: your actual work here.
			// For demo, just print:
			Console.WriteLine($"Processing PatientId={p.PatientId}, Name={p.Name}, DOB={p.DateOfBirth}");
			return Task.CompletedTask;
		}

		private static bool IsRetryable(Exception ex)
		{
			// e.g. network hiccups, transient Kafka exceptions, etc.
			if (ex is TimeoutException) return true;
			if (ex is KafkaException ke && !ke.Error.IsFatal) return true;
			return false;
		}
	}
}

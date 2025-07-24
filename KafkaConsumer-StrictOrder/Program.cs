using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Polly;
using Polly.Retry;

class OrderedKafkaConsumer
{
	private const string BootstrapServers = "your.kafka:9092";
	private const string Topic = "your-topic";
	private const string DeadLetterTopic = "your-topic.DLT";
	private const string GroupId = "ordered-consumer-group";

	static async Task Main()
	{
		var config = new ConsumerConfig
		{
			BootstrapServers = BootstrapServers,
			GroupId = GroupId,
			AutoOffsetReset = AutoOffsetReset.Earliest,
			EnableAutoCommit = false,               // manual commit
			IsolationLevel = IsolationLevel.ReadCommitted
		};

		// Polly retry policy: 3 attempts with exponential backoff
		AsyncRetryPolicy retryPolicy = Policy
			.Handle<Exception>(ex => IsRetryable(ex))
			.WaitAndRetryAsync(
				retryCount: 3,
				sleepDurationProvider: retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
				onRetry: (exception, timespan, retryCount, context) =>
				{
					Console.WriteLine($"Retry {retryCount} after {timespan}: {exception.Message}");
				});

		using var consumer = new ConsumerBuilder<string, string>(config).Build();
		using var deadLetterProducer = new ProducerBuilder<string, string>(
			new ProducerConfig { BootstrapServers = BootstrapServers }
		).Build();

		consumer.Subscribe(Topic);
		Console.CancelKeyPress += (_, e) => { e.Cancel = true; consumer.Close(); };

		Console.WriteLine("Consuming...");

		while (true)
		{
			try
			{
				var cr = consumer.Consume(CancellationToken.None);

				// Ensure strict order by blocking here until processing or dead-lettering finishes
				await ProcessWithRetries(cr, retryPolicy, deadLetterProducer, consumer);
			}
			catch (OperationCanceledException)
			{
				break;
			}
		}
	}

	private static async Task ProcessWithRetries(
		ConsumeResult<string, string> cr,
		AsyncRetryPolicy retryPolicy,
		IProducer<string, string> dlProducer,
		IConsumer<string, string> consumer)
	{
		try
		{
			// Wrap the business logic in the retry policy
			await retryPolicy.ExecuteAsync(async () =>
			{
				// Your message handling logic here
				await HandleMessageAsync(cr.Message.Key, cr.Message.Value);
			});

			// On success, commit the offset
			consumer.Commit(cr);
		}
		catch (Exception ex)
		{
			// Final exception: non-retryable or retries exhausted
			Console.WriteLine($"Moving to DLT: {ex.Message}");

			// Produce to dead-letter topic, including original metadata
			var dltMsg = new Message<string, string>
			{
				Key = cr.Message.Key,
				Value = cr.Message.Value,
				Headers = cr.Message.Headers
			};
			await dlProducer.ProduceAsync(DeadLetterTopic, dltMsg);

			// Commit so we skip this message next time
			consumer.Commit(cr);
		}
	}

	private static Task HandleMessageAsync(string key, string value)
	{
		// TODO: Replace with real processing. Throw to simulate failures.
		Console.WriteLine($"Processing message: {key} -> {value}");
		return Task.CompletedTask;
	}

	private static bool IsRetryable(Exception ex)
	{
		// Define which exceptions can be retried
		// e.g., network timeouts, transient errors, etc.
		return ex is TimeoutException
			   || ex is KafkaException ke && ke.Error.IsLocalError;
	}
}

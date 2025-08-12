using Confluent.Kafka;
using Polly;
using Polly.Retry;

namespace KafkaConsumer_StrictOrder;

class RefactoredConsole
{
	//const string Bootstrap = "localhost:9092";
	//const string SchemaReg = "localhost:8081";
	//const string Topic = "your-avro-topic";
	//const string DltTopic = "your-avro-topic.DLT";
	//const string GroupId = "consumer-group";

	private const string BootstrapServers = "localhost:9092";
	private const string SchemaRegistryUrl = "localhost:8081";
	private const string Topic = "your-avro-topic";
	private const string DeadLetterTopic = "your-avro-topic.DLT";
	private const string GroupId = "avro-ordered-consumer-group";

	public static async Task Start()
	{
		// -- build services (could be done via DI container) --
		using var topicMgr = new TopicManager(BootstrapServers);
		using var producer = new PatientProducer(BootstrapServers, SchemaRegistryUrl);
		using var consumer = new PatientConsumer(BootstrapServers, SchemaRegistryUrl, GroupId, DeadLetterTopic);

		// -- ensure topics --
		await topicMgr.EnsureTopicExistsAsync(Topic, partitions: 1, replicationFactor: 1);
		await topicMgr.EnsureTopicExistsAsync(DeadLetterTopic, partitions: 1, replicationFactor: 1);

		// -- start producer in background --
		var cts = new CancellationTokenSource();
		var prodTask = producer.ProduceSequenceAsync(Topic, count: 50);

		// -- define retry policy --
		AsyncRetryPolicy retryPolicy = Policy
			.Handle<Exception>(ex => ex is TimeoutException || (ex is KafkaException ke && !ke.Error.IsFatal))
			.WaitAndRetryAsync(3, i => TimeSpan.FromSeconds(Math.Pow(2, i)));

		// -- start consumer (blocks until canceled) --
		var consTask = consumer.ConsumeAsync(
			Topic,
			handlePatientAsync: p =>
			{
				Console.WriteLine($"Handling {p.PatientId}");
				return Task.CompletedTask;
			},
			retryPolicy,
			cts.Token
		);

		Console.WriteLine("Press Enter to stop...");
		Console.ReadLine();
		cts.Cancel();

		await Task.WhenAll(prodTask, consTask);

	}
}
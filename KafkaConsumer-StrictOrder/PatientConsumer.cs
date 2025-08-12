using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Example.PoC;
using Polly.Retry;

namespace KafkaConsumer_StrictOrder;

public class PatientConsumer : IPatientConsumer, IDisposable
{
	private readonly IConsumer<Ignore, Patient> _consumer;
	private readonly IProducer<string, Patient> _dlProducer;

	public PatientConsumer(
		string bootstrapServers,
		string schemaRegistryUrl,
		string groupId,
		string deadLetterTopic
	)
	{
		var registry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = schemaRegistryUrl });
		_consumer = new ConsumerBuilder<Ignore, Patient>(
				new ConsumerConfig
				{
					BootstrapServers = bootstrapServers,
					GroupId = groupId,
					AutoOffsetReset = AutoOffsetReset.Earliest,
					EnableAutoCommit = false,
					IsolationLevel = IsolationLevel.ReadCommitted
				})
			.SetValueDeserializer(new AvroDeserializer<Patient>(registry).AsSyncOverAsync())
			.Build();

		_dlProducer = new ProducerBuilder<string, Patient>(
				new ProducerConfig { BootstrapServers = bootstrapServers }
			)
			.SetValueSerializer(new AvroSerializer<Patient>(registry))
			.Build();

		DeadLetterTopic = deadLetterTopic;
	}

	public string DeadLetterTopic { get; }


	/*
		If the first _consumer.Commit(cr); throws an exception, what would happen to the message in Kafka?

			When a call to _consumer.Commit(cr); fails, the committed offset for that message isn’t advanced. Because your 
			consumer is configured with EnableAutoCommit = false, the message’s offset remains uncommitted, so if the 
			consumer restarts or a rebalance occurs, Kafka will redeliver the same message. 
			This behavior ensures that if a commit isn’t successful, the message will likely be processed again, 
			which might lead to duplicate processing unless additional handling is implemented.

		Would the ordering be affected?
	   
			The commit failure itself does not change the inherent ordering of messages in the Kafka partition. 
			Kafka always delivers messages in offset order within a partition. However, because the offset isn't 
			successfully updated, the consumer may redeliver the same message when it recovers, leading to duplicate 
			processing. If your processing logic doesn't handle these duplicates—or if a subsequent processing 
			attempt occurs in a different order—the effective behavior might seem out-of-order relative to your 
			application's expectations.
	   
			In summary, Kafka maintains partition order, but the failed commit can cause your application to 
			process a message more than once, potentially disrupting the expected processing flow if not handled 
			properly.

		What are the options for the consumer in this situation? I guess one option is that the message processing should be idempotent.....

			The consumer has a few options when a commit fails:
				
			1.	Idempotent Processing:
				Ensure that processing your message is idempotent. That way, if the message is processed more 
				than once because the offset was not correctly committed, the duplicate processing doesn't 
				lead to unintended side effects.
			2.	Reattempting the Commit:
				Instead of immediately moving to a dead-letter queue, you could implement additional retry logic 
				when the commit fails. This would reduce the chance of duplicate processing but may introduce delays.
			3.	Transactional Processing:
				Use Kafka's transactional features. This allows both message consumption and producing to a dead-letter 
				topic (or other downstream systems) to be part of a single atomic transaction, ensuring exactly once semantics. 
				However, this requires your Kafka setup and client libraries to support transactions.
			4.	Dead-letter Strategy:
				As shown in the code, pushing the message to a dead-letter queue is a valid option to prevent the 
				message from blocking further processing. Here, you still commit the offset for the failing message to 
				avoid redelivery, but you'll need to later handle or reprocess messages from the DLQ appropriately.
	   
			In all cases, ordering within a partition remains intact by Kafka, but the effective processing order can be 
			affected by duplicates or reprocessing if the commit isn’t performed correctly.

		Explain the message offset

			The message offset in Kafka is a unique identifier assigned to each message within a partition. Its main purposes are:
			•	Position Tracking: It acts as a bookmark, letting consumers know which messages have been processed so far.
			•	Fault Tolerance: By storing the last committed offset, the consumer can resume processing at the correct 
				position after a failure or restart.
			•	Avoiding Duplicates: A successfully committed offset prevents the consumer from redelivering already processed 
				messages (unless processing is idempotent and duplicates are acceptable).
	   
			In your code, because you disable auto-commit (EnableAutoCommit = false), you manually commit offsets. If the commit fails, 
			the offset isn’t advanced, and on restart or rebalance, Kafka will redeliver the message. This underscores the need for idempotent 
			processing or other commit retry mechanisms in scenarios where commit operations might fail.
	 */

	public async Task ConsumeAsync(
		string topic,
		Func<Patient, Task> handlePatientAsync,
		AsyncRetryPolicy retryPolicy,
		CancellationToken cancellation
	)
	{
		_consumer.Subscribe(topic);

		while (!cancellation.IsCancellationRequested)
		{
			var cr = _consumer.Consume(cancellation);
			try
			{
				// This handling should ideally be idempotent to avoid issues with retries after a commit failure.
				await retryPolicy.ExecuteAsync(() => handlePatientAsync(cr.Message.Value));

				/*
					The Confluent.Kafka library’s _consumer.Commit(cr); call does not include any built-in retry logic. If a commit 
					fails (for example, due to a transient issue), the method will throw an exception, and you'll need to handle the 
					retry behavior explicitly (e.g., using a retry policy or additional error handling) in your code.

					Therefore, we could add retry logic on the Commit to compensate for transient issues and reduce chance of failure 
				 */
				_consumer.Commit(cr);
				
				Console.WriteLine($"[Consumer] Committed {cr.Offset}");
			}
			catch (Exception ex)
			{
				// Naive implementation: Should add retry logic, logging, etc.
				Console.WriteLine($"[DLT] {cr.Offset} → {ex.Message}");
				
				await _dlProducer.ProduceAsync(
					DeadLetterTopic,
					new Message<string, Patient>
					{
						Key = cr.Message.Value.PatientId,
						Value = cr.Message.Value,
						Headers = cr.Message.Headers
					}, cancellation);

				_consumer.Commit(cr);
			}
		}
	}

	public void Dispose()
	{
		_consumer?.Close();
		_consumer?.Dispose();
		_dlProducer?.Dispose();
	}
}
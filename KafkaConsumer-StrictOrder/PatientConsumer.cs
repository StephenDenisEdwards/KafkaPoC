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
				await retryPolicy.ExecuteAsync(() => handlePatientAsync(cr.Message.Value));
				_consumer.Commit(cr);
				Console.WriteLine($"[Consumer] Committed {cr.Offset}");
			}
			catch (Exception ex)
			{
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
// PatientProducer.cs

using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Example.PoC;

namespace KafkaConsumer_StrictOrder;

public class PatientProducer : IPatientProducer, IDisposable
{
	private readonly IProducer<Null, Patient> _producer;

	public PatientProducer(string bootstrapServers, string schemaRegistryUrl)
	{
		var registry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = schemaRegistryUrl });
		_producer = new ProducerBuilder<Null, Patient>(
				new ProducerConfig { BootstrapServers = bootstrapServers }
			)
			.SetValueSerializer(new AvroSerializer<Patient>(registry))
			.Build();
	}

	public async Task ProduceSequenceAsync(string topic, int count)
	{
		for (int i = 1; i <= count; i++)
		{
			var p = new Patient
			{
				PatientId = i.ToString("D4"),
				Name = $"Patient {i}",
				DateOfBirth = DateTime.UtcNow.ToString("o")
			};
			await _producer.ProduceAsync(
				new TopicPartition(topic, partition: 0),
				new Message<Null, Patient> { Value = p }
			);
			Console.WriteLine($"[Producer] {p.PatientId}");
		}
		_producer.Flush(TimeSpan.FromSeconds(5));
	}

	public void Dispose() => _producer.Dispose();
}
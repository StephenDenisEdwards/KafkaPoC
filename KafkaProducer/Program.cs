using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Example.PoC;

class Program
{
	static async Task Main(string[] args)
	{
		var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://localhost:8081" };
		var producerConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };

		// Enable auto schema registration
		var avroSerializerConfig = new AvroSerializerConfig
		{
			AutoRegisterSchemas = true
		};

		using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
		using var producer = new ProducerBuilder<string, Patient>(producerConfig)
			.SetValueSerializer(new AvroSerializer<Patient>(schemaRegistry, avroSerializerConfig))
			.Build();

		for (int patientId = 123456; patientId < 123470; patientId++)
		{
			var patient = new Patient
			{
				PatientId = patientId.ToString(),
				Name = "John Doe",
				DateOfBirth = "1980-01-01"
			};

			var result = await producer.ProduceAsync("patient-topic", new Message<string, Patient>
			{
				Key = patient.PatientId,
				Value = patient
			});

			Console.WriteLine($"Sent Patient record to partition {result.Partition}, offset {result.Offset}");

			await Task.Delay(1000);
		}
	}
}
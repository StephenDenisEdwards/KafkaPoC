using System;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Example.PoC;


class Program
{
	static void Main(string[] args)
	{
		var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://localhost:8081" };
		var consumerConfig = new ConsumerConfig
		{
			BootstrapServers = "localhost:9092",
			GroupId = "patient-group",
			AutoOffsetReset = AutoOffsetReset.Earliest
		};

		using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
		using var consumer = new ConsumerBuilder<string, Patient>(consumerConfig)
			.SetValueDeserializer(new AvroDeserializer<Patient>(schemaRegistry)
			.AsSyncOverAsync())
			.Build();

		consumer.Subscribe("patient-topic");

		var cts = new CancellationTokenSource();
		Console.CancelKeyPress += (_, e) =>
		{
			e.Cancel = true;
			cts.Cancel();
		};

		try
		{
			while (!cts.Token.IsCancellationRequested)
			{
				var consumeResult = consumer.Consume(cts.Token);
				var patient = consumeResult.Message.Value;
				Console.WriteLine($"Received PatientId: {patient.PatientId}, Name: {patient.Name}, DOB: {patient.DateOfBirth}");
			}
		}
		catch (OperationCanceledException) { }
		finally
		{
			consumer.Close();
		}
	}
}
using System;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;

using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Xunit;
using Schema = Avro.Schema;

public class TopicNameStrategyTests
{
	// ---- Hardcoded config ----
	private const string BootstrapServers = "localhost:9092";
	private const string SchemaRegistryUrl = "http://localhost:8081";
	private const string Topic = "orders";

	// ---- Schemas ----
	private static readonly Schema KeySchemaV1 = Schema.Parse(@"
    {
      ""type"": ""record"",
      ""name"": ""OrderKey"",
      ""namespace"": ""demo.avro"",
      ""fields"": [
        { ""name"": ""order_id"", ""type"": ""string"" }
      ]
    }");

	private static readonly Schema ValueSchemaV1 = Schema.Parse(@"
    {
      ""type"": ""record"",
      ""name"": ""Order"",
      ""namespace"": ""demo.avro"",
      ""fields"": [
        { ""name"": ""orderId"", ""type"": ""string"" },
        { ""name"": ""customer"", ""type"": ""string"" },
        { ""name"": ""total"", ""type"": ""double"" }
      ]
    }");

	private static readonly Schema ValueSchemaV2 = Schema.Parse(@"
    {
      ""type"": ""record"",
      ""name"": ""Order"",
      ""namespace"": ""demo.avro"",
      ""fields"": [
        { ""name"": ""orderId"", ""type"": ""string"" },
        { ""name"": ""customer"", ""type"": ""string"" },
        { ""name"": ""total"", ""type"": ""double"" },
        { ""name"": ""status"", ""type"": [""null"", ""string""], ""default"": null }
      ]
    }");

	// ---- Helpers ----
	private static GenericRecord MakeKeyV1(string id)
	{
		var rec = new GenericRecord((RecordSchema)KeySchemaV1);
		rec.Add("order_id", id);
		return rec;
	}

	private static GenericRecord MakeValueV1(string id, string customer, double total)
	{
		var rec = new GenericRecord((RecordSchema)ValueSchemaV1);
		rec.Add("orderId", id);
		rec.Add("customer", customer);
		rec.Add("total", total);
		return rec;
	}

	private static GenericRecord MakeValueV2(string id, string customer, double total, string? status)
	{
		var rec = new GenericRecord((RecordSchema)ValueSchemaV2);
		rec.Add("orderId", id);
		rec.Add("customer", customer);
		rec.Add("total", total);
		rec.Add("status", status);
		return rec;
	}

	[Fact]
	public async Task Should_Create_TopicNameStrategy_Subjects_And_Evolve_Value()
	{
		using var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig
		{
			Url = SchemaRegistryUrl
		});

		var keyAvroCfg = new AvroSerializerConfig { SubjectNameStrategy = SubjectNameStrategy.Topic };
		var valAvroCfg = new AvroSerializerConfig { SubjectNameStrategy = SubjectNameStrategy.Topic };

		var producerCfg = new ProducerConfig
		{
			BootstrapServers = BootstrapServers,
			Acks = Acks.All,
			EnableIdempotence = true
		};

		using var producer =
			new ProducerBuilder<GenericRecord, GenericRecord>(producerCfg)
				.SetKeySerializer(new AvroSerializer<GenericRecord>(sr, keyAvroCfg))
				.SetValueSerializer(new AvroSerializer<GenericRecord>(sr, valAvroCfg))
				.Build();

		// Send first message (Key v1, Value v1)
		await producer.ProduceAsync(Topic, new Message<GenericRecord, GenericRecord>
		{
			Key = MakeKeyV1("order-001"),
			Value = MakeValueV1("order-001", "Alice", 49.99)
		});

		// Assert subjects exist
		var subjects = await sr.GetAllSubjectsAsync();
		Assert.Contains($"{Topic}-key", subjects);
		Assert.Contains($"{Topic}-value", subjects);

		// Schema versions
		var latestKey = await sr.GetLatestSchemaAsync($"{Topic}-key");
		var latestVal = await sr.GetLatestSchemaAsync($"{Topic}-value");
		Assert.Equal(1, latestKey.Version);
		Assert.Equal(1, latestVal.Version);

		// Send value evolution (backward compatible)
		await producer.ProduceAsync(Topic, new Message<GenericRecord, GenericRecord>
		{
			Key = MakeKeyV1("order-002"),
			Value = MakeValueV2("order-002", "Bob", 19.99, "CREATED")
		});

		latestKey = await sr.GetLatestSchemaAsync($"{Topic}-key");
		latestVal = await sr.GetLatestSchemaAsync($"{Topic}-value");
		Assert.Equal(1, latestKey.Version); // key schema unchanged
		Assert.Equal(2, latestVal.Version); // value schema bumped
	}

	[Fact]
	public async Task Should_Reject_Incompatible_Value_Change_Old()
	{
		using var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig
		{
			Url = SchemaRegistryUrl
		});

		var keyAvroCfg = new AvroSerializerConfig { SubjectNameStrategy = SubjectNameStrategy.Topic };
		var valAvroCfg = new AvroSerializerConfig { SubjectNameStrategy = SubjectNameStrategy.Topic };

		var producerCfg = new ProducerConfig
		{
			BootstrapServers = BootstrapServers,
			Acks = Acks.All,
			EnableIdempotence = true
		};

		// Ensure baseline schema exists
		using (var p =
			new ProducerBuilder<GenericRecord, GenericRecord>(producerCfg)
				.SetKeySerializer(new AvroSerializer<GenericRecord>(sr, keyAvroCfg))
				.SetValueSerializer(new AvroSerializer<GenericRecord>(sr, valAvroCfg))
				.Build())
		{
			await p.ProduceAsync(Topic, new Message<GenericRecord, GenericRecord>
			{
				Key = MakeKeyV1("order-100"),
				Value = MakeValueV1("order-100", "Eve", 10.0)
			});
		}

		// Force BACKWARD compatibility
		await sr.UpdateCompatibilityAsync( Confluent.SchemaRegistry.Compatibility.Backward, $"{Topic}-value");

		// Create incompatible schema (remove required field 'customer')
		var incompatible = Schema.Parse(@"
        {
          ""type"": ""record"",
          ""name"": ""Order"",
          ""namespace"": ""demo.avro"",
          ""fields"": [
            { ""name"": ""orderId"", ""type"": ""string"" }
          ]
        }");

		using var pBad =
			new ProducerBuilder<GenericRecord, GenericRecord>(producerCfg)
				.SetKeySerializer(new AvroSerializer<GenericRecord>(sr, keyAvroCfg))
				.SetValueSerializer(new AvroSerializer<GenericRecord>(sr, valAvroCfg))
				.Build();

		var badValue = new GenericRecord((RecordSchema)incompatible);
		badValue.Add("orderId", "order-999");

		await Assert.ThrowsAsync<Confluent.Kafka.ProduceException<GenericRecord, GenericRecord>>(async () =>
		{
			await pBad.ProduceAsync(Topic, new Message<GenericRecord, GenericRecord>
			{
				Key = MakeKeyV1("order-999"),
				Value = badValue
			});
		});
	}

	[Fact]
	public async Task Should_Reject_Incompatible_Value_Change()
	{
		using var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig
		{
			Url = SchemaRegistryUrl
		});

		// Force compatibility mode first
		await sr.UpdateCompatibilityAsync(Compatibility.Backward, $"{Topic}-value");

		var keyAvroCfg = new AvroSerializerConfig { SubjectNameStrategy = SubjectNameStrategy.Topic };
		var valAvroCfg = new AvroSerializerConfig { SubjectNameStrategy = SubjectNameStrategy.Topic };

		var producerCfg = new ProducerConfig
		{
			BootstrapServers = BootstrapServers,
			Acks = Acks.All,
			EnableIdempotence = true
		};

		// Ensure baseline version exists
		using (var p =
			new ProducerBuilder<GenericRecord, GenericRecord>(producerCfg)
				.SetKeySerializer(new AvroSerializer<GenericRecord>(sr, keyAvroCfg))
				.SetValueSerializer(new AvroSerializer<GenericRecord>(sr, valAvroCfg))
				.Build())
		{
			await p.ProduceAsync(Topic, new Message<GenericRecord, GenericRecord>
			{
				Key = MakeKeyV1("order-100"),
				Value = MakeValueV1("order-100", "Eve", 10.0)
			});
		}

		// Incompatible schema: removed required field "customer"
		var incompatible = Schema.Parse(@"
	    {
	      ""type"": ""record"",
	      ""name"": ""Order"",
	      ""namespace"": ""demo.avro"",
	      ""fields"": [
	        { ""name"": ""orderId"", ""type"": ""string"" }
	      ]
	    }");

		var badValue = new GenericRecord((RecordSchema)incompatible);
		badValue.Add("orderId", "order-999");

		using var pBad =
			new ProducerBuilder<GenericRecord, GenericRecord>(producerCfg)
				.SetKeySerializer(new AvroSerializer<GenericRecord>(sr, keyAvroCfg))
				.SetValueSerializer(new AvroSerializer<GenericRecord>(sr, valAvroCfg))
				.Build();

		// Expect SchemaRegistryException here
		await Assert.ThrowsAsync<SchemaRegistryException>(async () =>
		{
			await pBad.ProduceAsync(Topic, new Message<GenericRecord, GenericRecord>
			{
				Key = MakeKeyV1("order-999"),
				Value = badValue
			});
		});
	}

}

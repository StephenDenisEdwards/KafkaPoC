using System;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program
{
	static async Task Main(string[] args)
	{
		var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

		using var producer = new ProducerBuilder<Null, string>(config).Build();

		Console.WriteLine("Kafka Producer started, sending messages...");
		for (int i = 0; i < 10; i++)
		{
			var value = $"Hello Kafka! Message {i} at {DateTime.UtcNow}";
			var result = await producer.ProduceAsync("test-topic", new Message<Null, string> { Value = value });

			Console.WriteLine($"Sent '{value}' to partition {result.Partition}, offset {result.Offset}");
			await Task.Delay(1000);
		}

		producer.Flush(TimeSpan.FromSeconds(5));
	}
}


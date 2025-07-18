using System;
using System.Threading;
using Confluent.Kafka;

class Program
{
	static void Main(string[] args)
	{
		var config = new ConsumerConfig
		{
			BootstrapServers = "localhost:9092",
			GroupId = "my-group",
			AutoOffsetReset = AutoOffsetReset.Earliest
		};

		using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
		consumer.Subscribe("test-topic");

		Console.WriteLine("Kafka Consumer started, waiting for messages...");

		var cts = new CancellationTokenSource();

		Console.CancelKeyPress += (_, e) => {
			e.Cancel = true; // prevent immediate process exit
			cts.Cancel();
		};

		try
		{
			while (!cts.Token.IsCancellationRequested)
			{
				var cr = consumer.Consume(cts.Token);
				Console.WriteLine($"Received message '{cr.Message.Value}' from partition {cr.Partition}, offset {cr.Offset}");
			}
		}
		catch (OperationCanceledException) { }
		finally
		{
			consumer.Close();
		}
	}
}
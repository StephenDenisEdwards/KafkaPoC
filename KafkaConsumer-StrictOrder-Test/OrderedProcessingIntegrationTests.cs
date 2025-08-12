using KafkaConsumer_StrictOrder;
using Polly;
using Polly.Retry;
// your generated Patient class

namespace KafkaConsumer_StrictOrder_Test
{
	public class OrderedProcessingIntegrationTests
	{
		private const string BootstrapServers = "localhost:9092";
		private const string SchemaRegistryUrl = "localhost:8081";

		[Fact(Timeout = 60000)]
		public async Task ConsumerProcessesMessagesInStrictOrder()
		{
			// Arrange
			var topic = $"test-topic-{Guid.NewGuid():N}";
			var dlt = topic + ".DLT";
			const int messageCount = 20;

			// 1) Create topics
			var topicMgr = new TopicManager(BootstrapServers);
			await topicMgr.EnsureTopicExistsAsync(topic, 1, 1);
			await topicMgr.EnsureTopicExistsAsync(dlt, 1, 1);

			// 2) Produce sequence
			var producer = new PatientProducer(BootstrapServers, SchemaRegistryUrl);
			var produceTask = producer.ProduceSequenceAsync(topic, messageCount);

			// 3) Prepare consumer
			var consumerGroup = $"test-group-{Guid.NewGuid():N}";
			var consumer = new PatientConsumer(
				BootstrapServers,
				SchemaRegistryUrl,
				consumerGroup,
				dlt
			);

			// Capture in-order IDs
			var seenIds = new List<string>();
			var cts = new CancellationTokenSource();

			// 4) Retry policy with no delay (just for integration)
			AsyncRetryPolicy retryPolicy = Policy
				.Handle<Exception>()
				.WaitAndRetryAsync(
					retryCount: 3,
					sleepDurationProvider: _ => TimeSpan.Zero
				);

			// 5) Start consuming
			var consumeTask = consumer.ConsumeAsync(
				topic,
				async patient =>
				{
					seenIds.Add(patient.PatientId);
					if (seenIds.Count >= messageCount)
					{
						// stop once we've seen them all
						cts.Cancel();
					}
					await Task.CompletedTask;
				},
				retryPolicy,
				cts.Token
			);

			// Wait for both produce and consume to finish
			await Task.WhenAll(produceTask, consumeTask);

			// Assert
			Assert.Equal(messageCount, seenIds.Count);
			for (int i = 0; i < messageCount; i++)
			{
				var expected = (i + 1).ToString("D4");           // "0001", "0002", ...
				Assert.Equal(expected, seenIds[i]);
			}

			// Cleanup
			topicMgr.Dispose();
			producer.Dispose();
			consumer.Dispose();
		}
	}
}

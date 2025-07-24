using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaConsumer_StrictOrder;

public class TopicManager : ITopicManager, IDisposable
{
	private readonly IAdminClient _adminClient;

	public TopicManager(string bootstrapServers)
	{
		_adminClient = new AdminClientBuilder(
			new AdminClientConfig { BootstrapServers = bootstrapServers }
		).Build();
	}

	public async Task EnsureTopicExistsAsync(string topicName, int partitions, short replicationFactor)
	{
		var spec = new TopicSpecification
		{
			Name = topicName,
			NumPartitions = partitions,
			ReplicationFactor = replicationFactor
		};

		try
		{
			await _adminClient.CreateTopicsAsync(new[] { spec });
			Console.WriteLine($"[Admin] Created topic '{topicName}'.");
		}
		catch (CreateTopicsException e)
		{
			var result = e.Results?[0];
			if (result?.Error.Code == ErrorCode.TopicAlreadyExists)
				Console.WriteLine($"[Admin] Topic '{topicName}' already exists.");
			else
				throw;
		}
	}

	public void Dispose() => _adminClient.Dispose();
}
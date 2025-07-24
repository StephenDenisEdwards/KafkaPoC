namespace KafkaConsumer_StrictOrder;

public interface ITopicManager
{
	Task EnsureTopicExistsAsync(string topicName, int partitions, short replicationFactor);
}
// ITopicManager.cs

namespace KafkaConsumer_StrictOrder;

public interface IPatientProducer
{
	Task ProduceSequenceAsync(string topic, int count);
}


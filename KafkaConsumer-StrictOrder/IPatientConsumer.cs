using Polly.Retry;

namespace KafkaConsumer_StrictOrder;

public interface IPatientConsumer
{
	Task ConsumeAsync(
		string topic,
		Func<Example.PoC.Patient, Task> handlePatientAsync,
		AsyncRetryPolicy retryPolicy,
		CancellationToken cancellation
	);
}
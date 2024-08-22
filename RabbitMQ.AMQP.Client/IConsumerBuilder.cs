namespace RabbitMQ.AMQP.Client;

public enum StreamOffsetSpecification
{
    First,
    Last,
    Next
}

// TODO IAddressBuilder<IConsumerBuilder>?
public interface IConsumerBuilder
{
    IConsumerBuilder Queue(IQueueSpecification queueSpecification);
    IConsumerBuilder Queue(string queueName);

    IConsumerBuilder MessageHandler(MessageHandler handler);

    IConsumerBuilder InitialCredits(int initialCredits);

    IStreamOptions Stream();

    Task<IConsumer> BuildAsync(CancellationToken cancellationToken = default);

    public interface IStreamOptions
    {
        IStreamOptions Offset(long offset);

        // IStreamOptions offset(Instant timestamp);

        IStreamOptions Offset(StreamOffsetSpecification specification);

        IStreamOptions Offset(string interval);

        IStreamOptions FilterValues(string[] values);

        IStreamOptions FilterMatchUnfiltered(bool matchUnfiltered);

        IConsumerBuilder Builder();
    }
}

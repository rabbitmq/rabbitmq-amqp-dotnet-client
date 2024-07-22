namespace RabbitMQ.AMQP.Client;


public enum StreamOffsetSpecification
{
    First,
    Last,
    Next
}

public interface IConsumerBuilder
{
    IConsumerBuilder Queue(string queue);

    IConsumerBuilder MessageHandler(MessageHandler handler);

    IConsumerBuilder InitialCredits(int initialCredits);

    IStreamOptions Stream();

    IConsumer Build();

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

namespace RabbitMQ.AMQP.Client;

public enum QueueType
{
    QUORUM,
    CLASSIC,
    STREAM
}

public interface IQueueInfo
{
    string Name();

    bool Durable();

    bool AutoDelete();

    bool Exclusive();

    QueueType Type();

    Dictionary<string, object> Arguments();

    string Leader();

    List<string> Replicas();

    ulong MessageCount();

    uint ConsumerCount();
}
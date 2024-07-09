namespace RabbitMQ.AMQP.Client;

public enum QueueType
{
    QUORUM,
    CLASSIC,
    STREAM
}

public interface IQueueInfo : IEntityInfo
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


public enum ExchangeType
{
    DIRECT,
    FANOUT,
    TOPIC,
    HEADERS
}

public interface IExchangeInfo : IEntityInfo
{
}

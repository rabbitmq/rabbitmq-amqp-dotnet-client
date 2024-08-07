namespace RabbitMQ.AMQP.Client;

public interface IEntityInfo
{
}

/// <summary>
/// Generic interface for declaring entities with result of type T
/// </summary>
/// <typeparam name="T"></typeparam>
public interface IEntityInfoDeclaration<T> where T : IEntityInfo
{
    // TODO this really should be named DeclareAsync
    Task<T> Declare();
}

/// <summary>
/// Generic interface for declaring entities without result
/// </summary>
public interface IEntityDeclaration
{
    // TODO this really should be named DeclareAsync
    Task Declare();
}

public enum OverFlowStrategy
{
    DropHead,
    RejectPublish,

    RejectPublishDlx
    // DROP_HEAD("drop-head"),
    // REJECT_PUBLISH("reject-publish"),
    // REJECT_PUBLISH_DLX("reject-publish-dlx");
}

public interface IQueueSpecification : IEntityInfoDeclaration<IQueueInfo>
{
    IQueueSpecification Name(string name);
    public string Name();

    IQueueSpecification Exclusive(bool exclusive);
    public bool Exclusive();

    IQueueSpecification AutoDelete(bool autoDelete);
    public bool AutoDelete();

    IQueueSpecification Arguments(Dictionary<object, object> arguments);
    public Dictionary<object, object> Arguments();

    IQueueSpecification Type(QueueType type);
    public QueueType Type();

    IQueueSpecification DeadLetterExchange(string dlx);

    IQueueSpecification DeadLetterRoutingKey(string dlrk);

    IQueueSpecification OverflowStrategy(OverFlowStrategy overflow);

    IQueueSpecification MaxLengthBytes(ByteCapacity maxLengthBytes);

    IQueueSpecification SingleActiveConsumer(bool singleActiveConsumer);

    IQueueSpecification Expires(TimeSpan expiration);

    IStreamSpecification Stream();

    IQuorumQueueSpecification Quorum();

    IClassicQueueSpecification Classic();

    IQueueSpecification MaxLength(long maxLength);

    IQueueSpecification MessageTtl(TimeSpan ttl);
}

public interface IStreamSpecification
{
    public IStreamSpecification MaxAge(TimeSpan maxAge);

    public IStreamSpecification MaxSegmentSizeBytes(ByteCapacity maxSegmentSize);

    public IStreamSpecification InitialClusterSize(int initialClusterSize);

    public IQueueSpecification Queue();
}

public enum QuorumQueueDeadLetterStrategy
{
    // AT_MOST_ONCE("at-most-once"),
    // AT_LEAST_ONCE("at-least-once");
    AtMostOnce,
    AtLeastOnce
}

public interface IQuorumQueueSpecification
{
    IQuorumQueueSpecification DeadLetterStrategy(QuorumQueueDeadLetterStrategy strategy);

    IQuorumQueueSpecification DeliveryLimit(int limit);

    IQuorumQueueSpecification QuorumInitialGroupSize(int size);

    IQueueSpecification Queue();
}

public enum ClassicQueueMode
{
    Default,
    Lazy
}

public enum ClassicQueueVersion
{
    // V1(1),
    // V2(2);
    V1,
    V2
}

public interface IClassicQueueSpecification
{
    // 1 <= maxPriority <= 255
    IClassicQueueSpecification MaxPriority(int maxPriority);

    IClassicQueueSpecification Mode(ClassicQueueMode mode);

    IClassicQueueSpecification Version(ClassicQueueVersion version);

    IQueueSpecification Queue();
}

public interface IQueueDeletion
{
    // TODO consider returning a QueueStatus object with some info after deletion
    // TODO should be named DeleteAsync and take CancellationToken
    Task<IEntityInfo> Delete(string name);
}

public interface IExchangeSpecification : IEntityDeclaration
{
    IExchangeSpecification Name(string name);

    IExchangeSpecification AutoDelete(bool autoDelete);

    IExchangeSpecification Type(ExchangeType type);

    IExchangeSpecification Type(string type); // TODO: Add this

    IExchangeSpecification Argument(string key, object value);
}

public interface IExchangeDeletion
{
    // TODO consider returning a ExchangeStatus object with some info after deletion
    // TODO should be named DeleteAsync and take CancellationToken
    Task Delete(string name);
}

public interface IBindingSpecification
{
    IBindingSpecification SourceExchange(string exchange);

    IBindingSpecification DestinationQueue(string queue);

    IBindingSpecification DestinationExchange(string exchange);

    IBindingSpecification Key(string key);

    IBindingSpecification Argument(string key, object value);

    IBindingSpecification Arguments(Dictionary<string, object> arguments);

    Task Bind();
    Task Unbind();
}

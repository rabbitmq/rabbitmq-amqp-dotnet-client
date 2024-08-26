// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client;

public interface IEntityInfo
{
}

/// <summary>
/// Generic interface for managing entities with result of type T
/// </summary>
/// <typeparam name="T"></typeparam>
public interface IEntityInfoSpecification<T> where T : IEntityInfo
{
    Task<T> DeclareAsync();
    Task<T> DeleteAsync();
}

/// <summary>
/// Generic interface for specifying entities without result
/// </summary>
public interface IEntitySpecification
{
    Task DeclareAsync();
    Task DeleteAsync();
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

public interface IQueueSpecification : IEntityInfoSpecification<IQueueInfo>
{
    public string Name();
    IQueueSpecification Name(string name);

    public bool Exclusive();
    IQueueSpecification Exclusive(bool exclusive);

    public bool AutoDelete();
    IQueueSpecification AutoDelete(bool autoDelete);

    public Dictionary<object, object> Arguments();
    IQueueSpecification Arguments(Dictionary<object, object> arguments);

    public QueueType Type();
    IQueueSpecification Type(QueueType type);

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

public interface IExchangeSpecification : IEntitySpecification
{
    string Name();
    IExchangeSpecification Name(string name);

    IExchangeSpecification AutoDelete(bool autoDelete);

    bool AutoDelete();

    IExchangeSpecification Type(ExchangeType type);

    ExchangeType Type();

    IExchangeSpecification Argument(string key, object value);
    Dictionary<string, object> Arguments();

    IExchangeSpecification Arguments(Dictionary<string, object> arguments);
}

public interface IBindingSpecification
{
    IBindingSpecification SourceExchange(IExchangeSpecification exchangeSpec);

    IBindingSpecification SourceExchange(string exchangeName);
    string SourceExchangeName();

    IBindingSpecification DestinationQueue(IQueueSpecification queueSpec);
    IBindingSpecification DestinationQueue(string queueName);
    string DestinationQueueName();

    IBindingSpecification DestinationExchange(IExchangeSpecification exchangeSpec);
    IBindingSpecification DestinationExchange(string exchangeName);
    string DestinationExchangeName();

    IBindingSpecification Key(string key);
    string Key();

    IBindingSpecification Argument(string key, object value);

    IBindingSpecification Arguments(Dictionary<string, object> arguments);
    Dictionary<string, object> Arguments();

    string Path();

    Task BindAsync();
    Task UnbindAsync();
}

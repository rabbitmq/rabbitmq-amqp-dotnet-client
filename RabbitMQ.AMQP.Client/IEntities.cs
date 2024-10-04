// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
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
        public string QueueName { get; }
        IQueueSpecification Name(string queueName);

        public bool IsExclusive { get; }
        IQueueSpecification Exclusive(bool isExclusive);

        public bool IsAutoDelete { get; }
        IQueueSpecification AutoDelete(bool isAutoDelete);

        public Dictionary<object, object> QueueArguments { get; }
        IQueueSpecification Arguments(Dictionary<object, object> queueArguments);

        public QueueType QueueType { get; }
        IQueueSpecification Type(QueueType queueType);

        IQueueSpecification DeadLetterExchange(string dlx);

        IQueueSpecification DeadLetterRoutingKey(string dlrk);

        IQueueSpecification OverflowStrategy(OverFlowStrategy overflow);

        IQueueSpecification MaxLengthBytes(ByteCapacity maxLengthBytes);

        // TODO: Add more tests for SingleActiveConsumer
        IQueueSpecification SingleActiveConsumer(bool singleActiveConsumer);

        IQueueSpecification Expires(TimeSpan expiration);

        IQueueSpecification MaxLength(long maxLength);

        IQueueSpecification MessageTtl(TimeSpan ttl);

        IStreamSpecification Stream();

        IQuorumQueueSpecification Quorum();

        IClassicQueueSpecification Classic();

        Task<ulong> PurgeAsync();
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
        string ExchangeName { get; }
        IExchangeSpecification Name(string exchangeName);

        bool IsAutoDelete { get; }
        IExchangeSpecification AutoDelete(bool isAutoDelete);

        string ExchangeType { get; }
        IExchangeSpecification Type(ExchangeType exchangeType);
        IExchangeSpecification Type(string exchangeType);

        Dictionary<string, object> ExchangeArguments { get; }
        IExchangeSpecification Argument(string key, object value);
        IExchangeSpecification Arguments(Dictionary<string, object> arguments);
    }

    public interface IBindingSpecification
    {
        IBindingSpecification SourceExchange(IExchangeSpecification exchangeSpec);

        string SourceExchangeName { get; }
        IBindingSpecification SourceExchange(string exchangeName);

        string DestinationQueueName { get; }
        IBindingSpecification DestinationQueue(IQueueSpecification queueSpec);
        IBindingSpecification DestinationQueue(string queueName);

        string DestinationExchangeName { get; }
        IBindingSpecification DestinationExchange(IExchangeSpecification exchangeSpec);
        IBindingSpecification DestinationExchange(string exchangeName);

        string BindingKey { get; }
        IBindingSpecification Key(string bindingKey);

        Dictionary<string, object> BindingArguments { get; }
        IBindingSpecification Argument(string key, object value);
        IBindingSpecification Arguments(Dictionary<string, object> arguments);

        string BindingPath { get; }

        Task BindAsync();
        Task UnbindAsync();
    }
}

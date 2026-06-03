// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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

    public enum LeaderLocatorStrategy
    {
        ClientLocal,
        Balanced
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

        IQueueSpecification Type(QueueType queueType);

        IQueueSpecification DeadLetterExchange(string dlx);

        IQueueSpecification DeadLetterRoutingKey(string dlrk);

        IQueueSpecification OverflowStrategy(OverFlowStrategy overflow);

        IQueueSpecification MaxLengthBytes(ByteCapacity maxLengthBytes);

        IQueueSpecification LeaderLocator(LeaderLocatorStrategy strategy);

        IQueueSpecification SingleActiveConsumer(bool singleActiveConsumer);

        IQueueSpecification Expires(TimeSpan expiration);

        IQueueSpecification MaxLength(long maxLength);

        IQueueSpecification MessageTtl(TimeSpan ttl);

        IStreamSpecification Stream();

        IQuorumQueueSpecification Quorum();

        /// <summary>
        ///   Configures a JMS queue (<c>x-queue-type: jms</c>). Requires a broker that supports the JMS queue type.
        /// </summary>
        IJmsQueueSpecification Jms();

        IClassicQueueSpecification Classic();

        Task<ulong> PurgeAsync();
    }

    public interface IStreamSpecification
    {
        public IStreamSpecification MaxAge(TimeSpan maxAge);

        public IStreamSpecification MaxSegmentSizeBytes(ByteCapacity maxSegmentSize);

        public IStreamSpecification InitialClusterSize(int initialClusterSize);

        public IStreamSpecification FileSizePerChunk(ByteCapacity fileSizePerChunk);

        public IQueueSpecification Queue();
    }

    public enum QuorumQueueDeadLetterStrategy
    {
        // AT_MOST_ONCE("at-most-once"),
        // AT_LEAST_ONCE("at-least-once");
        AtMostOnce,
        AtLeastOnce
    }

    /// <summary>
    /// Conditions for delaying a message when it is returned to a quorum queue.
    /// <para>
    /// The delay is calculated with linear back-off based on the message's delivery count:
    /// <c>min(min_delay * delivery_count, max_delay)</c>. A per-message explicit delivery time
    /// can also be set by adding the <c>x-opt-delivery-time</c> annotation (a Unix timestamp in
    /// milliseconds) when requeuing a message with annotations.
    /// </para>
    /// <para>Delayed retry support for quorum queues is available as of RabbitMQ 4.3.</para>
    /// </summary>
    /// <seealso href="https://www.rabbitmq.com/docs/quorum-queues#delayed-retry">Delayed Retry</seealso>
    public enum QuorumQueueDelayedRetryType
    {
        /// <summary>Delayed retry is not applied (default).</summary>
        Disabled,

        // All and Failed are not ready yet.
        // We will make it available in a future releases

        /// <summary>All returned messages are delayed, regardless of whether the delivery count was incremented.</summary>
        [Obsolete("This status is not ready to use.", true)]
        All,

        /// <summary>
        /// Only messages with an incremented <c>delivery-count</c> are delayed.
        /// This happens for example when discarding a message via <c>IContext.Discard()</c>.
        /// </summary>
        [Obsolete("This status is not ready to use.", true)]
        Failed,

        /// <summary>
        /// Only messages without an incremented <c>delivery-count</c> are delayed.
        /// This happens for example when requeuing a message via <c>IContext.Requeue()</c>.
        /// </summary>
        Returned
    }

    public interface IQuorumQueueSpecification
    {
        IQuorumQueueSpecification DeadLetterStrategy(QuorumQueueDeadLetterStrategy strategy);

        IQuorumQueueSpecification DeliveryLimit(int limit);

        IQuorumQueueSpecification QuorumInitialGroupSize(int size);

        IQuorumQueueSpecification QuorumTargetGroupSize(int size);

        /// <summary>
        ///   Sets the <c>x-consumer-timeout</c> queue argument (milliseconds).
        /// </summary>
        IQuorumQueueSpecification ConsumerTimeout(TimeSpan timeout);

        /// <summary>
        /// Set the delayed retry type.
        /// <para>
        /// Defines the conditions for delaying a message when it is returned to the queue.
        /// You must also call <see cref="DelayedRetryMin"/> to configure the minimum retry delay.
        /// </para>
        /// <para>Delayed retry support for quorum queues requires RabbitMQ 4.3+.</para>
        /// </summary>
        /// <param name="type">The delayed retry condition.</param>
        /// <seealso href="https://www.rabbitmq.com/docs/quorum-queues#delayed-retry">Delayed Retry</seealso>
        IQuorumQueueSpecification DelayedRetryType(QuorumQueueDelayedRetryType type);

        /// <summary>
        /// Set the minimum delay for delayed retry (in milliseconds).
        /// <para>
        /// The delay grows linearly with the delivery count: <c>min(min * delivery_count, max)</c>.
        /// </para>
        /// </summary>
        /// <param name="min">Minimum retry delay. Must be positive.</param>
        /// <seealso href="https://www.rabbitmq.com/docs/quorum-queues#delayed-retry">Delayed Retry</seealso>
        IQuorumQueueSpecification DelayedRetryMin(TimeSpan min);

        /// <summary>
        /// Set the maximum delay for delayed retry (in milliseconds).
        /// </summary>
        /// <param name="max">Maximum retry delay. Must be positive.</param>
        /// <seealso href="https://www.rabbitmq.com/docs/quorum-queues#delayed-retry">Delayed Retry</seealso>
        IQuorumQueueSpecification DelayedRetryMax(TimeSpan max);

        IQueueSpecification Queue();
    }

    /// <summary>
    ///   Optional arguments for JMS queues (<c>x-queue-type: jms</c>).
    /// </summary>
    public interface IJmsQueueSpecification
    {
        /// <summary>
        ///   Sets the <c>x-consumer-timeout</c> queue argument (milliseconds).
        /// </summary>
        IJmsQueueSpecification ConsumerTimeout(TimeSpan timeout);

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

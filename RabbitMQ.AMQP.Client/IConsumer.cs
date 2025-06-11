// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    // TODO cancellation token
    /// <summary>
    /// Delegate to process an incoming message.
    /// </summary>
    /// <param name="context">The message context.</param>
    /// <param name="message">The message itself.</param>
    /// <returns><see cref="Task"/> that represents the async operation.</returns>
    public delegate Task MessageHandler(IContext context, IMessage message);

    /// <summary>
    /// <para>API to consume messages from a RabbitMQ queue.</para>
    /// <para>Instances are configured and created with a <see cref="IConsumerBuilder"/>.</para>
    /// <para>See <see cref="IConnection.ConsumerBuilder()"/> and <see cref="IConsumerBuilder"/>.</para>
    /// </summary>
    public interface IConsumer : ILifeCycle
    {
        /// <summary>
        /// Pause the consumer to stop receiving messages.
        /// </summary>
        void Pause();

        /// <summary>
        /// Request to receive messages again.
        /// </summary>
        void Unpause();

        /// <summary>
        /// Returns the number of unsettled messages.
        /// </summary>
        long UnsettledMessageCount { get; }
    }

    public interface IContext
    {
        /// <summary>
        /// <para>Accept the message (AMQP 1.0 <c>accepted</c> outcome).</para>
        /// <para>This means the message has been processed and the broker can delete it.</para>
        /// </summary>
        void Accept();

        ///<summary>
        /// <para>Discard the message (AMQP 1.0 <c>rejected</c> outcome).</para>
        /// <para>
        ///   This means the message cannot be processed because it is invalid, the broker can
        ///   drop it or dead-letter it if it is configured.
        /// </para>
        ///</summary>
        void Discard();

        ///<summary>
        /// <para>
        ///   Discard the message with annotations to combine with the existing message annotations.
        /// </para>
        /// <para>
        ///   This means the message cannot be processed because it is invalid, the broker can drop
        ///   it or dead-letter it if it is configured.
        /// </para>
        /// <para>
        ///   Application-specific annotation keys must start with the <c>x-opt-</c> prefix.
        /// </para>
        /// <para>
        ///   Annotation keys that the broker understands start with <c>x-</c>, but not with
        ///   <c>x-opt-</c>. This maps to the AMQP 1.0 <c>modified{delivery-failed = false,
        ///   undeliverable-here = false}</c> outcome.
        /// </para>
        /// <para>
        ///   The annotations can be used only with Quorum queues, see
        ///   <a href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">
        ///   AMQP 1.0 <c>modified</c> outcome.</a>
        /// </para>
        /// <param name="annotations">Message annotations to combine with existing ones.</param>
        ///</summary>
        void Discard(Dictionary<string, object> annotations);

        ///<summary>
        ///Requeue the message (AMQP 1.0 <code>released</code> outcome).
        ///
        ///This means the message has not been processed and the broker can requeue it and deliver it
        /// to the same or a different consumer.
        ///
        /// </summary>
        void Requeue();

        ///<summary>
        /// <para>
        ///   Requeue the message with annotations to combine with the existing message annotations.
        /// </para>
        /// <para>
        ///   This means the message has not been processed and the broker can requeue it and
        ///   deliver it to the same or a different consumer.
        /// </para>
        /// <para>
        ///   Application-specific annotation keys must start with the <c>x-opt-</c> prefix.
        /// </para>
        /// <para>
        ///   Annotation keys that the broker understands start with <c>x-</c>, but not with
        ///   <c>x-opt-</c>. This maps to the AMQP 1.0 <c>modified{delivery-failed = false,
        ///   undeliverable-here = false}</c> outcome.
        /// </para>
        /// <para>
        ///   The annotations can be used only with Quorum queues, see
        ///   <a href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">
        ///   AMQP 1.0 <c>modified</c> outcome.</a>
        /// </para>
        /// <param name="annotations">Message annotations to combine with existing ones.</param>
        ///</summary>
        void Requeue(Dictionary<string, object> annotations);

        /// <summary>
        /// Create a batch context to accumulate message contexts and settle them at once.
        /// The message context the batch context is created from is <b>not</b> added to the batch
        /// context.
        /// @return the created batch context
        /// </summary>
        IBatchContext Batch();
    }

    public interface IBatchContext : IContext
    {
        /// <summary>
        /// Add a message context to the batch context.
        /// @param context the message context to add
        /// </summary>
        void Add(IContext context);

        /// <summary>
        /// Get the current number of message contexts in the batch context.
        /// @return current number of message contexts in the batch
        /// </summary>
        int Count();
    }
}

// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    public class ConsumerException : Exception
    {
        public ConsumerException(string message) : base(message)
        {
        }
    }

    public delegate Task MessageHandler(IContext context, IMessage message);

    public interface IConsumer : ILifeCycle
    {
        void Pause();
        void Unpause();
        long UnsettledMessageCount { get; }
    }

    public interface IContext
    {
        ///<summary>
        /// Accept the message (AMQP 1.0 <code>accepted</code> outcome).
        ///
        /// This means the message has been processed and the broker can delete it.
        /// 
        /// </summary>
        Task AcceptAsync();

        ///<summary>
        /// Discard the message (AMQP 1.0 <code>rejected</code> outcome).
        ///This means the message cannot be processed because it is invalid, the broker can drop it
        /// or dead-letter it if it is configured.
        ///</summary>
        Task DiscardAsync();

        ///<summary>
        ///Discard the message with annotations to combine with the existing message annotations.
        ///This means the message cannot be processed because it is invalid, the broker can drop it
        ///or dead-letter it if it is configured.
        ///Application-specific annotation keys must start with the <code>x-opt-</code> prefix.
        ///Annotation keys the broker understands starts with <code>x-</code>, but not with <code>x-opt-
        ///</code>.
        ///This maps to the AMQP 1.0 <code>
        ///modified{delivery-failed = true, undeliverable-here = true}</code> outcome.
        /// <param name="annotations"> annotations message annotations to combine with existing ones </param>
        ///<a
        ///    href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
        ///    1.0 <code>modified</code> outcome</a>
        ///
        /// The annotations can be used only with Quorum queues, see https://www.rabbitmq.com/docs/amqp#modified-outcome
        ///</summary>
        Task DiscardAsync(Dictionary<string, object> annotations);
        ///<summary>
        ///Requeue the message (AMQP 1.0 <code>released</code> outcome).
        ///
        ///This means the message has not been processed and the broker can requeue it and deliver it
        /// to the same or a different consumer.
        ///
        /// </summary>
        Task RequeueAsync();

        ///<summary>
        ///Requeue the message with annotations to combine with the existing message annotations.
        /// 
        ///This means the message has not been processed and the broker can requeue it and deliver it
        /// to the same or a different consumer.
        /// Application-specific annotation keys must start with the <code>x-opt-</code> prefix.
        /// Annotation keys the broker understands starts with <code>x-</code>, but not with <code>x-opt-
        /// </code>.
        ///
        /// This maps to the AMQP 1.0 <code>
        /// modified{delivery-failed = false, undeliverable-here = false}</code> outcome.
        ///
        /// <param name="annotations"> annotations message annotations to combine with existing ones </param>
        ///<a
        ///     href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-modified">AMQP
        ///     1.0 <code>modified</code> outcome</a>
        ///
        ///The annotations can be used only with Quorum queues, see https://www.rabbitmq.com/docs/amqp#modified-outcome
        ///</summary>
        Task RequeueAsync(Dictionary<string, object> annotations);
    }
}

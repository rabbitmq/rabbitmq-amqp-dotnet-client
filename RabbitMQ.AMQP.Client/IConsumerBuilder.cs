// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
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

        /// <summary>
        /// SubscriptionListener interface callback to add behavior before a subscription is created.
        /// This callback is meant for stream consumers:
        /// it can be used to dynamically set the offset the consumer attaches to in the stream.
        /// It is called when the consumer is first created and when the client has to re-subscribe
        /// (e.g. after a disconnection).
        /// </summary>
        /// <param name="listenerContext"> Contains the listenerContext, see <see cref="ListenerContext"/>  </param>
        /// <returns></returns>
        IConsumerBuilder SubscriptionListener(Action<ListenerContext> listenerContext);

        IStreamOptions Stream();

        Task<IConsumer> BuildAndStartAsync(CancellationToken cancellationToken = default);


        public interface IStreamOptions
        {
            IStreamOptions Offset(long offset);
            IStreamOptions Offset(StreamOffsetSpecification specification);
            IStreamOptions FilterValues(string[] values);
            IStreamOptions FilterMatchUnfiltered(bool matchUnfiltered);
            IConsumerBuilder Builder();
        }


        /// <summary>
        ///  ListenerContext is a helper class that holds the contexts for the listener
        /// </summary>
        /// <param name="StreamOptions"> Stream Options that the user can change during the SubscriptionListener </param>
        public record ListenerContext(IStreamOptions StreamOptions)
        {
            public IStreamOptions StreamOptions { get; } = StreamOptions;
        }
    }
}

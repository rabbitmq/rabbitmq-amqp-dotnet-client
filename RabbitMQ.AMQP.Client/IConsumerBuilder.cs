// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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

        IStreamOptions Stream();

        Task<IConsumer> BuildAndStartAsync(CancellationToken cancellationToken = default);

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
}

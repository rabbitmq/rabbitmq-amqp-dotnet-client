// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client
{
    public interface IMetricsReporter
    {
        enum PublishDispositionValue
        {
            ACCEPTED,
            REJECTED,
            RELEASED
        };

        enum ConsumeDispositionValue
        {
            ACCEPTED,
            DISCARDED,
            REQUEUED
        };

        void ConnectionOpened();
        void ConnectionClosed();

        void PublisherOpened();
        void PublisherClosed();

        void ConsumerOpened();
        void ConsumerClosed();

        void Published();
        void PublishDisposition(PublishDispositionValue disposition);

        void Consumed();
        void ConsumeDisposition(ConsumeDispositionValue disposition);
    }
}

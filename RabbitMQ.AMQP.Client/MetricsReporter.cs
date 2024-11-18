// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Diagnostics.Metrics;
using System.Threading;

namespace RabbitMQ.AMQP.Client
{
    // .NET docs on metric instrumentation: https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics-instrumentation
    // OpenTelemetry semantic conventions for messaging metric: https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics
    public sealed class MetricsReporter : IMetricsReporter
    {
        private int _connectionCount = 0;
        private readonly Gauge<int> _connections;

        private int _publisherCount = 0;
        private readonly Gauge<int> _publishers;

        private int _consumerCount = 0;
        private readonly Gauge<int> _consumers;

        private readonly Counter<int> _published;
        private readonly Histogram<double> _publishDuration;
        private readonly Counter<int> _publishAccepted;
        private readonly Counter<int> _publishRejected;
        private readonly Counter<int> _publishReleased;

        private readonly Counter<int> _consumed;
        private readonly Histogram<double> _consumedDuration;
        private readonly Counter<int> _consumeAccepted;
        private readonly Counter<int> _consumeRequeued;
        private readonly Counter<int> _consumeDiscarded;

        private const string Version = "0.1.0";

        public const string MetricPrefix = "rabbitmq.amqp";

        public const string MeterName = "RabbitMQ.Amqp";

        public MetricsReporter(IMeterFactory meterFactory)
        {
            Meter meter = meterFactory.Create(MeterName, Version);

            _connections = meter.CreateGauge<int>(
                MetricPrefix + ".connections",
                description:
                "The total number of connections to the broker in this AMQP environment.");

            _publishers = meter.CreateGauge<int>(
                MetricPrefix + ".publishers",
                description:
                "The total number of publishers.");

            _consumers = meter.CreateGauge<int>(
                MetricPrefix + ".consumers",
                description:
                "The total number of consumers.");

            _published = meter.CreateCounter<int>(
                MetricPrefix + ".published",
                description:
                "The total number of messages published to the broker.");

            _publishDuration = meter.CreateHistogram<double>(
                MetricPrefix + ".published.duration",
                unit: "ms",
                description: "Elapsed time between publishing a message and receiving a broker response, in milliseconds.");

            _publishAccepted = meter.CreateCounter<int>(
                MetricPrefix + ".published_accepted",
                description:
                "The total number of messages published and accepted by the broker.");

            _publishRejected = meter.CreateCounter<int>(
                MetricPrefix + ".published_rejected",
                description:
                "The total number of messages published and rejected by the broker.");

            _publishReleased = meter.CreateCounter<int>(
                MetricPrefix + ".published_released",
                description:
                "The total number of messages published and released by the broker.");

            _consumed = meter.CreateCounter<int>(
                MetricPrefix + ".consumed",
                description:
                "The total number of messages consumed to the broker.");

            _consumedDuration = meter.CreateHistogram<double>(
                MetricPrefix + ".consumed.duration",
                unit: "ms",
                description: "Elapsed time of receiving a message and handling it, in milliseconds.");

            _consumeAccepted = meter.CreateCounter<int>(
                MetricPrefix + ".consumed_accepted",
                description:
                "The total number of messages consumed and accepted by the broker.");

            _consumeRequeued = meter.CreateCounter<int>(
                MetricPrefix + ".consumed_requeued",
                description:
                "The total number of messages consumed and requeued by the broker.");

            _consumeDiscarded = meter.CreateCounter<int>(
                MetricPrefix + ".consumed_discarded",
                description:
                "The total number of messages consumed and discarded by the broker.");
        }

        public void ConnectionOpened()
        {
            _connections.Record(Interlocked.Increment(ref _connectionCount));
        }

        public void ConnectionClosed()
        {
            _connections.Record(Interlocked.Decrement(ref _connectionCount));
        }

        public void PublisherOpened()
        {
            _publishers.Record(Interlocked.Increment(ref _publisherCount));
        }

        public void PublisherClosed()
        {
            _publishers.Record(Interlocked.Decrement(ref _publisherCount));
        }

        public void ConsumerOpened()
        {
            _consumers.Record(Interlocked.Increment(ref _consumerCount));
        }

        public void ConsumerClosed()
        {
            _consumers.Record(Interlocked.Decrement(ref _consumerCount));
        }

        public void Published(TimeSpan elapsed)
        {
            _published.Add(1);
            _publishDuration.Record(elapsed.TotalMilliseconds);
        }

        public void PublishDisposition(IMetricsReporter.PublishDispositionValue disposition)
        {
            switch (disposition)
            {
                case IMetricsReporter.PublishDispositionValue.ACCEPTED:
                    _publishAccepted.Add(1);
                    break;
                case IMetricsReporter.PublishDispositionValue.REJECTED:
                    _publishRejected.Add(1);
                    break;
                case IMetricsReporter.PublishDispositionValue.RELEASED:
                    _publishReleased.Add(1);
                    break;
            }
        }

        public void Consumed(TimeSpan elapsed)
        {
            _consumed.Add(1);
            _consumedDuration.Record(elapsed.TotalMilliseconds);
        }

        public void ConsumeDisposition(IMetricsReporter.ConsumeDispositionValue disposition)
        {
            switch (disposition)
            {
                case IMetricsReporter.ConsumeDispositionValue.ACCEPTED:
                    _consumeAccepted.Add(1);
                    break;
                case IMetricsReporter.ConsumeDispositionValue.DISCARDED:
                    _consumeDiscarded.Add(1);
                    break;
                case IMetricsReporter.ConsumeDispositionValue.REQUEUED:
                    _consumeRequeued.Add(1);
                    break;
            }
        }
    }
}

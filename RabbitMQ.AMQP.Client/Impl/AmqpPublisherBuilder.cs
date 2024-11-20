// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client.Impl
{
    public class AmqpPublisherBuilder : IPublisherBuilder
    {
        private readonly AmqpConnection _connection;
        private string? _exchange = null;
        private string? _key = null;
        private string? _queue = null;
        private TimeSpan _timeout = TimeSpan.FromSeconds(10);
        private readonly IMetricsReporter? _metricsReporter;

        public AmqpPublisherBuilder(AmqpConnection connection, IMetricsReporter? metricsReporter)
        {
            _connection = connection;
            _metricsReporter = metricsReporter;
        }

        public IPublisherBuilder Exchange(IExchangeSpecification exchangeSpec)
        {
            return Exchange(exchangeSpec.ExchangeName);
        }

        public IPublisherBuilder Exchange(string exchangeName)
        {
            _exchange = exchangeName;
            return this;
        }

        public IPublisherBuilder Queue(IQueueSpecification queueSpec)
        {
            return Queue(queueSpec.QueueName);
        }

        public IPublisherBuilder Queue(string queueName)
        {
            _queue = queueName;
            return this;
        }

        public IPublisherBuilder Key(string key)
        {
            _key = key;
            return this;
        }

        // TODO this is unused, and should be done via a CancellationToken on PublishAsync anyway
        public IPublisherBuilder PublishTimeout(TimeSpan timeout)
        {
            _timeout = timeout;
            return this;
        }

        private bool IsAnonymous()
        {
            return string.IsNullOrEmpty(_exchange) && string.IsNullOrEmpty(_queue) && string.IsNullOrEmpty(_key);
        }

        public async Task<IPublisher> BuildAsync(CancellationToken cancellationToken = default)
        {
            string? address = null;
            if (!IsAnonymous())
            {
                address = AddressBuilderHelper.AddressBuilder().Exchange(_exchange).Queue(_queue).Key(_key).Address();
            }

            AmqpPublisher publisher = new(_connection, address, _metricsReporter);

            // TODO pass cancellationToken
            await publisher.OpenAsync()
                .ConfigureAwait(false);

            return publisher;
        }
    }
}

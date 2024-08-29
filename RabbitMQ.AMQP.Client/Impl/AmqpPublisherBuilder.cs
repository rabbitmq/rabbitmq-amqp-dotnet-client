// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client.Impl
{
    public class AddressBuilder : IAddressBuilder<AddressBuilder>
    {
        private string? _exchange;

        private string? _queue;

        private string? _key;

        public AddressBuilder Exchange(IExchangeSpecification exchangeSpec)
        {
            return Exchange(exchangeSpec.ExchangeName);
        }

        public AddressBuilder Exchange(string? exchange)
        {
            _exchange = exchange;
            return this;
        }

        public AddressBuilder Queue(IQueueSpecification queueSpec)
        {
            return Queue(queueSpec.QueueName);
        }

        public AddressBuilder Queue(string? queue)
        {
            _queue = queue;
            return this;
        }

        public AddressBuilder Key(string? key)
        {
            _key = key;
            return this;
        }

        public string Address()
        {
            if (_exchange == null && _queue == null)
            {
                throw new InvalidAddressException("Exchange or Queue must be set");
            }

            if (_exchange != null && _queue != null)
            {
                throw new InvalidAddressException("Exchange and Queue cannot be set together");
            }

            if (_exchange != null)
            {
                if (string.IsNullOrEmpty(_exchange))
                {
                    throw new InvalidAddressException("Exchange must be set");
                }

                if (_key != null && false == string.IsNullOrEmpty(_key))
                {
                    return $"/{Consts.Exchanges}/{Utils.EncodePathSegment(_exchange)}/{Utils.EncodePathSegment(_key)}";
                }

                return $"/{Consts.Exchanges}/{Utils.EncodePathSegment(_exchange)}";
            }

            if (_queue == null)
            {
                return "";
            }

            if (string.IsNullOrEmpty(_queue))
            {
                throw new InvalidAddressException("Queue must be set");
            }

            return $"/{Consts.Queues}/{Utils.EncodePathSegment(_queue)}";
        }
    }

    public class AmqpPublisherBuilder : IPublisherBuilder
    {
        private readonly AmqpConnection _connection;
        private string? _exchange;
        private string? _key;
        private string? _queue;
        private TimeSpan _timeout = TimeSpan.FromSeconds(10);

        public AmqpPublisherBuilder(AmqpConnection connection)
        {
            _connection = connection;
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

        public IPublisherBuilder PublishTimeout(TimeSpan timeout)
        {
            _timeout = timeout;
            return this;
        }


        public async Task<IPublisher> BuildAsync(CancellationToken cancellationToken = default)
        {
            string address = new AddressBuilder().Exchange(_exchange).Queue(_queue).Key(_key).Address();

            AmqpPublisher publisher = new(_connection, address, _timeout);

            // TODO pass cancellationToken
            await publisher.OpenAsync()
                .ConfigureAwait(false);

            return publisher;
        }
    }
}

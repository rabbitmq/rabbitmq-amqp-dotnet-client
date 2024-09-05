// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl
{
    public class AmqpConsumerBuilder : IConsumerBuilder
    {
        private readonly AmqpConnection _connection;
        private string _queue = "";
        private int _initialCredits = 10;
        private readonly Map _filters = new Map();
        private MessageHandler? _handler;

        public AmqpConsumerBuilder(AmqpConnection connection)
        {
            _connection = connection;
        }

        public IConsumerBuilder Queue(IQueueSpecification queueSpec)
        {
            return Queue(queueSpec.QueueName);
        }

        public IConsumerBuilder Queue(string queueName)
        {
            _queue = queueName;
            return this;
        }

        public IConsumerBuilder MessageHandler(MessageHandler handler)
        {
            _handler = handler;
            return this;
        }

        public IConsumerBuilder InitialCredits(int initialCredits)
        {
            _initialCredits = initialCredits;
            return this;
        }

        public IConsumerBuilder.IStreamOptions Stream()
        {
            return new DefaultStreamOptions(this, _filters);
        }

        public async Task<IConsumer> BuildAndStartAsync(CancellationToken cancellationToken = default)
        {
            if (_handler is null)
            {
                throw new ConsumerException("Message handler is not set");
            }

            string address = new AddressBuilder().Queue(_queue).Address();

            AmqpConsumer consumer = new(_connection, address, _handler, _initialCredits, _filters);

            // TODO pass cancellationToken
            await consumer.OpenAsync()
                .ConfigureAwait(false);

            return consumer;
        }
    }

    public class DefaultStreamOptions : IConsumerBuilder.IStreamOptions
    {
        private readonly IConsumerBuilder _consumerBuilder;
        private readonly Map _filters;

        public DefaultStreamOptions(IConsumerBuilder consumerBuilder, Map filters)
        {
            _consumerBuilder = consumerBuilder;
            _filters = filters;
        }

        public IConsumerBuilder.IStreamOptions Offset(long offset)
        {
            _filters[new Symbol("rabbitmq:stream-offset-spec")] = offset;
            return this;
        }

        // public IConsumerBuilder.IStreamOptions Offset(Instant timestamp)
        // {
        //     notNull(timestamp, "Timestamp offset cannot be null");
        //     this.offsetSpecification(JSType.Date.from(timestamp));
        //     return this;
        // }

        public IConsumerBuilder.IStreamOptions Offset(StreamOffsetSpecification specification)
        {
            // notNull(specification, "Offset specification cannot be null");
            OffsetSpecification(specification.ToString().ToLower());
            return this;
        }

        public IConsumerBuilder.IStreamOptions Offset(string interval)
        {
            // notNull(interval, "Interval offset cannot be null");
            // if (!Utils.validateMaxAge(interval))
            // {
            //     throw new IllegalArgumentException(
            //         "Invalid value for interval: "
            //         + interval
            //         + ". "
            //         + "Valid examples are: 1Y, 7D, 10m. See https://www.rabbitmq.com/docs/streams#retention.");
            // }

            OffsetSpecification(interval);
            return this;
        }

        public IConsumerBuilder.IStreamOptions FilterValues(string[] values)
        {
            _filters[new Symbol("rabbitmq:stream-filter")] = values.ToList();
            return this;
        }


        public IConsumerBuilder.IStreamOptions FilterMatchUnfiltered(bool matchUnfiltered)
        {
            _filters[new Symbol("rabbitmq:stream-match-unfiltered")] = matchUnfiltered;
            return this;
        }

        public IConsumerBuilder Builder()
        {
            return _consumerBuilder;
        }

        private void OffsetSpecification(object value)
        {
            _filters[new Symbol("rabbitmq:stream-offset-spec")] = value;
        }
    }
}

// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
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
        private readonly Map _filters = new();
        private MessageHandler? _handler;
        private Action<IConsumerBuilder.ListenerContext>? _listenerContext = null;

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

        public IConsumerBuilder SubscriptionListener(Action<IConsumerBuilder.ListenerContext> context)
        {
            _listenerContext = context;
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

            if (_listenerContext is not null)
            {
                _filters.Clear();

                _listenerContext(new IConsumerBuilder.ListenerContext(new DefaultStreamOptions(this, _filters)));
            }

            AmqpConsumer consumer = new(_connection, address, _handler, _initialCredits, _filters);

            // TODO pass cancellationToken
            await consumer.OpenAsync()
                .ConfigureAwait(false);

            return consumer;
        }
    }


    public abstract class ListenerStreamOptions : IConsumerBuilder.IStreamOptions
    {
        protected Map _filters;

        protected ListenerStreamOptions(Map filters)
        {
            _filters = filters;
        }


        public IConsumerBuilder.IStreamOptions Offset(long offset)
        {
            _filters[new Symbol("rabbitmq:stream-offset-spec")] = offset;
            return this;
        }

        public IConsumerBuilder.IStreamOptions Offset(StreamOffsetSpecification specification)
        {
            OffsetSpecification(specification.ToString().ToLower());
            return this;
        }

        public IConsumerBuilder.IStreamOptions Offset(string interval)
        {
            OffsetSpecification(interval);
            return this;
        }

        private void OffsetSpecification(object value)
        {
            _filters[new Symbol("rabbitmq:stream-offset-spec")] = value;
        }

        public IConsumerBuilder.IStreamOptions FilterValues(string[] values)
        {
            this._filters[new Symbol("rabbitmq:stream-filter")] = values;
            return this;
        }

        public IConsumerBuilder.IStreamOptions FilterMatchUnfiltered(bool matchUnfiltered)
        {
            this._filters[new Symbol("rabbitmq:stream-match-unfiltered")] = matchUnfiltered;
            return this;
        }

        public abstract IConsumerBuilder Builder();
    }

    public class DefaultStreamOptions : ListenerStreamOptions, IConsumerBuilder.IStreamOptions
    {
        private readonly IConsumerBuilder _consumerBuilder;

        public DefaultStreamOptions(IConsumerBuilder consumerBuilder, Map filters) : base(filters)
        {
            _consumerBuilder = consumerBuilder;
        }


        public override IConsumerBuilder Builder()
        {
            return _consumerBuilder;
        }
    }
}

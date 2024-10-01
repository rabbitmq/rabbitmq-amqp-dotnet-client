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
    /// <summary>
    /// ConsumerConfiguration is a helper class that holds the configuration for the consumer
    /// </summary>
    public class ConsumerConfiguration
    {
        public AmqpConnection Connection { get; set; } = null!;
        public string Address { get; set; } = "";
        public int InitialCredits { get; set; } = 10;
        public Map Filters { get; set; } = new();
        public MessageHandler? Handler { get; set; }
        public Action<IConsumerBuilder.ListenerContext>? ListenerContext = null;
    }

    public class AmqpConsumerBuilder : IConsumerBuilder
    {
        private readonly ConsumerConfiguration _configuration = new();

        public AmqpConsumerBuilder(AmqpConnection connection)
        {
            _configuration.Connection = connection;
        }

        public IConsumerBuilder Queue(IQueueSpecification queueSpec)
        {
            return Queue(queueSpec.QueueName);
        }

        public IConsumerBuilder Queue(string queueName)
        {
            string address = new AddressBuilder().Queue(queueName).Address();
            _configuration.Address = address;
            return this;
        }

        public IConsumerBuilder MessageHandler(MessageHandler handler)
        {
            _configuration.Handler = handler;
            return this;
        }

        public IConsumerBuilder InitialCredits(int initialCredits)
        {
            _configuration.InitialCredits = initialCredits;
            return this;
        }

        public IConsumerBuilder SubscriptionListener(Action<IConsumerBuilder.ListenerContext> context)
        {
            _configuration.ListenerContext = context;
            return this;
        }


        public IConsumerBuilder.IStreamOptions Stream()
        {
            return new ConsumerBuilderStreamOptions(this, _configuration.Filters);
        }


        public async Task<IConsumer> BuildAndStartAsync(CancellationToken cancellationToken = default)
        {
            if (_configuration.Handler is null)
            {
                throw new ConsumerException("Message handler is not set");
            }


            AmqpConsumer consumer = new(_configuration);

            // TODO pass cancellationToken
            await consumer.OpenAsync()
                .ConfigureAwait(false);

            return consumer;
        }
    }


    /// <summary>
    /// The base class for the stream options.
    /// The class set the right filters used to create the consumer
    /// See also <see cref="ListenerStreamOptions"/> and <see cref="ConsumerBuilderStreamOptions"/>
    /// </summary>
    public abstract class StreamOptions : IConsumerBuilder.IStreamOptions
    {
        private readonly Map _filters;

        protected StreamOptions(Map filters)
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
            _filters[new Symbol("rabbitmq:stream-filter")] = values.ToList();
            return this;
        }

        public IConsumerBuilder.IStreamOptions FilterMatchUnfiltered(bool matchUnfiltered)
        {
            _filters[new Symbol("rabbitmq:stream-match-unfiltered")] = matchUnfiltered;
            return this;
        }

        public abstract IConsumerBuilder Builder();
    }


    /// <summary>
    /// The stream options for the Subscribe Listener event.
    /// For the user perspective, it is used to set the stream options for the listener
    /// </summary>
    public class ListenerStreamOptions : StreamOptions
    {
        public ListenerStreamOptions(Map filters) : base(filters)
        {
        }

        /// <summary>
        /// This method is not implemented for the listener stream options
        /// Since it is not needed for the listener
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public override IConsumerBuilder Builder() => throw new NotImplementedException();
    }

    /// <summary>
    /// The class that implements the stream options for the consumer builder
    /// It is used to set the stream options for the consumer builder
    /// </summary>
    public class ConsumerBuilderStreamOptions : StreamOptions
    {
        private readonly IConsumerBuilder _consumerBuilder;

        public ConsumerBuilderStreamOptions(IConsumerBuilder consumerBuilder, Map filters) : base(filters)
        {
            _consumerBuilder = consumerBuilder;
        }


        public override IConsumerBuilder Builder()
        {
            return _consumerBuilder;
        }
    }
}

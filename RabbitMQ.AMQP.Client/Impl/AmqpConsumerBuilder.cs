// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl
{
    /// <summary>
    /// ConsumerConfiguration is a helper class that holds the configuration for the consumer
    /// </summary>
    internal sealed class ConsumerConfiguration
    {
        public string Address { get; set; } = "";
        public int InitialCredits { get; set; } = 100; // TODO use constant, check with Java lib
        public Map Filters { get; set; } = new();
        public MessageHandler? Handler { get; set; }
        // TODO re-name to ListenerContextAction? Callback?
        public Action<IConsumerBuilder.ListenerContext>? ListenerContext = null;
    }

    /// <summary>
    /// The builder class for create the consumer.
    /// The builder is called by the connection
    /// </summary>
    public class AmqpConsumerBuilder : IConsumerBuilder
    {
        private readonly ConsumerConfiguration _configuration = new();
        private readonly AmqpConnection _amqpConnection;
        private readonly IMetricsReporter? _metricsReporter;

        public AmqpConsumerBuilder(AmqpConnection connection, IMetricsReporter? metricsReporter)
        {
            _amqpConnection = connection;
            _metricsReporter = metricsReporter;
        }

        public IConsumerBuilder Queue(IQueueSpecification queueSpec)
        {
            return Queue(queueSpec.QueueName);
        }

        public IConsumerBuilder Queue(string queueName)
        {
            string address = AddressBuilderHelper.AddressBuilder().Queue(queueName).Address();
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
            return new ConsumerBuilderStreamOptions(this, _configuration.Filters,
                _amqpConnection.AreFilterExpressionsSupported);
        }

        public async Task<IConsumer> BuildAndStartAsync(CancellationToken cancellationToken = default)
        {
            if (_configuration.Handler is null)
            {
                throw new ConsumerException("Message handler is not set");
            }

            AmqpConsumer consumer = new(_amqpConnection, _configuration, _metricsReporter);

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
        private static readonly Regex s_offsetValidator = new Regex("^[0-9]+[YMDhms]$",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        private const string RmqStreamFilter = "rabbitmq:stream-filter";
        private const string RmqStreamOffsetSpec = "rabbitmq:stream-offset-spec";
        private const string RmqStreamMatchUnfiltered = "rabbitmq:stream-match-unfiltered";

        private static readonly Symbol s_streamFilterSymbol = new(RmqStreamFilter);
        private static readonly Symbol s_streamOffsetSpecSymbol = new(RmqStreamOffsetSpec);
        private static readonly Symbol s_streamMatchUnfilteredSymbol = new(RmqStreamMatchUnfiltered);

        private readonly Map _filters;
        private readonly bool _areFilterExpressionsSupported;

        protected StreamOptions(Map filters, bool areFilterExpressionsSupported)
        {
            _filters = filters;
            _areFilterExpressionsSupported = areFilterExpressionsSupported;
        }

        public IConsumerBuilder.IStreamOptions Offset(long offset)
        {
            _filters[s_streamOffsetSpecSymbol] =
                new DescribedValue(s_streamOffsetSpecSymbol, offset);
            return this;
        }

        public IConsumerBuilder.IStreamOptions Offset(DateTime timestamp)
        {
            SetOffsetSpecificationFilter(timestamp);
            return this;
        }

        public IConsumerBuilder.IStreamOptions Offset(StreamOffsetSpecification specification)
        {
            SetOffsetSpecificationFilter(specification.ToString().ToLowerInvariant());
            return this;
        }

        public IConsumerBuilder.IStreamOptions Offset(string interval)
        {
            if (string.IsNullOrWhiteSpace(interval))
            {
                throw new ArgumentNullException(nameof(interval));
            }

            if (false == s_offsetValidator.IsMatch(interval))
            {
                throw new ArgumentOutOfRangeException(nameof(interval));
            }

            SetOffsetSpecificationFilter(interval);
            return this;
        }

        public IConsumerBuilder.IStreamOptions FilterValues(params string[] values)
        {
            _filters[s_streamFilterSymbol] =
                new DescribedValue(s_streamFilterSymbol, values.ToList());
            return this;
        }

        public IConsumerBuilder.IStreamOptions FilterMatchUnfiltered(bool matchUnfiltered)
        {
            _filters[s_streamMatchUnfilteredSymbol]
                = new DescribedValue(s_streamMatchUnfilteredSymbol, matchUnfiltered);
            return this;
        }

        public abstract IConsumerBuilder Builder();

        private void SetOffsetSpecificationFilter(object value)
        {
            _filters[s_streamOffsetSpecSymbol]
                = new DescribedValue(s_streamOffsetSpecSymbol, value);
        }

        public IConsumerBuilder.IStreamFilterOptions Filter()
        {
            /*
             * TODO detect RMQ version
             *    if (!this.builder.connection.filterExpressionsSupported()) {
             *      throw new IllegalArgumentException(
             *          "AMQP filter expressions requires at least RabbitMQ 4.1.0");
             *    }
             */
            // TODO Should this be Consumer / Listener?
            return new StreamFilterOptions(this, _filters);
        }
    }

    /// <summary>
    /// The stream options for the Subscribe Listener event.
    /// For the user perspective, it is used to set the stream options for the listener
    /// </summary>
    public class ListenerStreamOptions : StreamOptions
    {
        public ListenerStreamOptions(Map filters, bool areFilterExpressionsSupported)
            : base(filters, areFilterExpressionsSupported)
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

        public ConsumerBuilderStreamOptions(IConsumerBuilder consumerBuilder,
            Map filters, bool areFilterExpressionsSupported)
            : base(filters, areFilterExpressionsSupported)
        {
            _consumerBuilder = consumerBuilder;
        }

        public override IConsumerBuilder Builder()
        {
            return _consumerBuilder;
        }
    }

    /// <summary>
    /// The base class for the stream filter options.
    /// The class set the right stream filters used to create the consumer
    /// </summary>
    public class StreamFilterOptions : IConsumerBuilder.IStreamFilterOptions
    {
        private IConsumerBuilder.IStreamOptions _streamOptions;
        private Map _filters;

        public StreamFilterOptions(IConsumerBuilder.IStreamOptions streamOptions, Map filters)
        {
            _streamOptions = streamOptions;
            _filters = filters;
        }

        public IConsumerBuilder.IStreamOptions Stream()
        {
            return _streamOptions;
        }

        public IConsumerBuilder.IStreamFilterOptions MessageId(object id)
            => PropertyFilter("message-id", id);

        public IConsumerBuilder.IStreamFilterOptions UserId(byte[] userId)
            => PropertyFilter("user-id", userId);

        public IConsumerBuilder.IStreamFilterOptions To(string to)
            => PropertyFilter("to", to);

        public IConsumerBuilder.IStreamFilterOptions Subject(string subject)
            => PropertyFilter("subject", subject);

        public IConsumerBuilder.IStreamFilterOptions ReplyTo(string replyTo)
            => PropertyFilter("reply-to", replyTo);

        public IConsumerBuilder.IStreamFilterOptions CorrelationId(object correlationId)
            => PropertyFilter("correlation-id", correlationId);

        public IConsumerBuilder.IStreamFilterOptions AbsoluteExpiryTime(DateTime absoluteExpiryTime)
            => PropertyFilter("absolute-expiry-time", absoluteExpiryTime);

        public IConsumerBuilder.IStreamFilterOptions ContentEncoding(string contentEncoding)
            => PropertyFilter("content-encoding", new Symbol(contentEncoding));

        public IConsumerBuilder.IStreamFilterOptions ContentType(string contentType)
            => PropertyFilter("content-type", new Symbol(contentType));

        public IConsumerBuilder.IStreamFilterOptions CreationTime(DateTime creationTime)
            => PropertyFilter("creation-time", creationTime);

        public IConsumerBuilder.IStreamFilterOptions GroupId(string groupId)
            => PropertyFilter("group-id", groupId);

        public IConsumerBuilder.IStreamFilterOptions GroupSequence(uint groupSequence)
            => PropertyFilter("group-sequence", groupSequence);

        public IConsumerBuilder.IStreamFilterOptions ReplyToGroupId(string groupId) =>
            PropertyFilter("reply-to-group-id", groupId);

        public IConsumerBuilder.IStreamFilterOptions Property(string key, object value)
            => ApplicationPropertyFilter(key, value);

        public IConsumerBuilder.IStreamFilterOptions PropertySymbol(string key, string value)
            => ApplicationPropertyFilter(key, new Symbol(value));

        private StreamFilterOptions PropertyFilter(string propertyKey, object propertyValue)
        {
            const string AmqpPropertiesFilter = "amqp:properties-filter";

            DescribedValue propertiesFilterValue = Filter(AmqpPropertiesFilter);
            Map propertiesFilter = (Map)propertiesFilterValue.Value;
            // Note: you MUST use a symbol as the key
            propertiesFilter.Add(new Symbol(propertyKey), propertyValue);
            return this;
        }

        private StreamFilterOptions ApplicationPropertyFilter(string propertyKey, object propertyValue)
        {
            const string AmqpApplicationPropertiesFilter = "amqp:application-properties-filter";

            DescribedValue applicationPropertiesFilterValue = Filter(AmqpApplicationPropertiesFilter);
            Map applicationPropertiesFilter = (Map)applicationPropertiesFilterValue.Value;
            // Note: do NOT put a symbol as the key
            applicationPropertiesFilter.Add(propertyKey, propertyValue);
            return this;
        }

        private DescribedValue Filter(string filterName)
        {
            var filterNameSymbol = new Symbol(filterName);

            if (false == _filters.ContainsKey(filterNameSymbol))
            {
                _filters[filterNameSymbol] = new DescribedValue(filterNameSymbol, new Map());
            }

            return (DescribedValue)_filters[filterNameSymbol];
        }
    }
}

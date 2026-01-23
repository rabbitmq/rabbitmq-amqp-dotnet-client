// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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

        public string? Queue { get; set; } = null;
        public int InitialCredits { get; set; } = 100; // TODO use constant, check with Java lib

        public Map Filters { get; set; } = new();

        // TODO is a MessageHandler *really* optional???
        public MessageHandler? Handler { get; set; }

        /// <summary>
        /// If direct reply-to is enabled, the client will use the direct reply-to feature of AMQP 1.0.
        /// The server must also support direct reply-to.
        /// This feature allows the server to send the reply directly to the client without going through a reply queue.
        /// This can improve performance and reduce latency.
        /// Default is false.
        /// https://www.rabbitmq.com/docs/direct-reply-to
        /// </summary>
        public bool DirectReplyTo { get; set; }

        /// <summary>
        /// If pre-settled is enabled, the receiver will use ReceiverSettleMode.Second,
        /// meaning messages are pre-settled and the receiver does not need to explicitly settle them.
        /// Default is false.
        /// </summary>
        public bool PreSettled { get; set; }

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

        public IConsumerBuilder Queue(string? queueName)
        {
            _configuration.Queue = queueName;
            return this;
        }

        public IConsumerBuilder MessageHandler(MessageHandler handler)
        {
            _configuration.Handler = handler;
            return this;
        }

        public IConsumerBuilder DirectReplyTo(bool directReplyTo)
        {
            _configuration.DirectReplyTo = directReplyTo;
            return this;
        }

        public IConsumerBuilder InitialCredits(int initialCredits)
        {
            _configuration.InitialCredits = initialCredits;
            return this;
        }

        public IConsumerBuilder PreSettled(bool preSettled)
        {
            _configuration.PreSettled = preSettled;
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

            if (_configuration.Filters[Consts.s_sqlFilterSymbol] is not null &&
                !_amqpConnection._featureFlags.IsSqlFeatureEnabled)
            {
                throw new NotSupportedException("SQL filter is not supported by the connection. " +
                                                "RabbitMQ 4.2.0 or later is required.");
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

        private readonly Map _filters;

        protected StreamOptions(Map filters)
        {
            _filters = filters;
        }

        public IConsumerBuilder.IStreamOptions Offset(long offset)
        {
            _filters[Consts.s_streamOffsetSpecSymbol] =
                new DescribedValue(Consts.s_streamOffsetSpecSymbol, offset);
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
            _filters[Consts.s_streamFilterSymbol] =
                new DescribedValue(Consts.s_streamFilterSymbol, values.ToList());
            return this;
        }

        public IConsumerBuilder.IStreamOptions FilterMatchUnfiltered(bool matchUnfiltered)
        {
            _filters[Consts.s_streamMatchUnfilteredSymbol]
                = new DescribedValue(Consts.s_streamMatchUnfilteredSymbol, matchUnfiltered);
            return this;
        }

        public abstract IConsumerBuilder Builder();

        private void SetOffsetSpecificationFilter(object value)
        {
            _filters[Consts.s_streamOffsetSpecSymbol]
                = new DescribedValue(Consts.s_streamOffsetSpecSymbol, value);
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
        public ListenerStreamOptions(Map filters)
            : base(filters)
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
            Map filters)
            : base(filters)
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
        private readonly IConsumerBuilder.IStreamOptions _streamOptions;
        private readonly Map _filters;

        public StreamFilterOptions(IConsumerBuilder.IStreamOptions streamOptions, Map filters)
        {
            _streamOptions = streamOptions;
            _filters = filters;
        }

        public IConsumerBuilder.IStreamFilterOptions Sql(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentNullException(nameof(sql));
            }

            _filters[Consts.s_sqlFilterSymbol] =
                new DescribedValue(Consts.s_streamSqlFilterSymbol, sql);
            return this;
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

            DescribedValue propertiesFilterValue = Filter(Consts.AmqpPropertiesFilter);
            Map propertiesFilter = (Map)propertiesFilterValue.Value;
            // Note: you MUST use a symbol as the key
            propertiesFilter.Add(new Symbol(propertyKey), propertyValue);
            return this;
        }

        private StreamFilterOptions ApplicationPropertyFilter(string propertyKey, object propertyValue)
        {

            DescribedValue applicationPropertiesFilterValue = Filter(Consts.AmqpApplicationPropertiesFilter);
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

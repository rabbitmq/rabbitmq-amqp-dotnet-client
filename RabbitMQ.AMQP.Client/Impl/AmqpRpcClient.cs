using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl
{
    public class RpcClientConfiguration
    {
        public AmqpConnection Connection { get; set; } = null!;
        public string ReplyToQueue { get; set; } = "";
        public string RequestAddress { get; set; } = "";
        public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);

        public Func<object>? CorrelationIdSupplier { get; set; } = null;

        public Func<IMessage, object>? CorrelationIdExtractor { get; set; }
    }

    public class AmqpRpcClientBuilder : IRpcClientBuilder
    {
        private readonly RpcClientAddressBuilder _addressBuilder;
        private readonly AmqpConnection _connection;
        private readonly RpcClientConfiguration _configuration = new();

        public AmqpRpcClientBuilder(AmqpConnection connection)
        {
            _connection = connection;
            _addressBuilder = new RpcClientAddressBuilder(this);
        }

        public IRpcClientAddressBuilder RequestAddress()
        {
            return _addressBuilder;
        }

        public IRpcClientBuilder ReplyToQueue(string replyToQueue)
        {
            _configuration.ReplyToQueue = replyToQueue;
            return this;
        }

        public IRpcClientBuilder CorrelationIdExtractor(Func<IMessage, object> correlationIdExtractor)
        {
            _configuration.CorrelationIdExtractor = correlationIdExtractor;
            return this;
        }

        public IRpcClientBuilder CorrelationIdSupplier(Func<object> correlationIdSupplier)
        {
            _configuration.CorrelationIdSupplier = correlationIdSupplier;
            return this;
        }

        public IRpcClientBuilder Timeout(TimeSpan timeout)
        {
            _configuration.Timeout = timeout;
            return this;
        }

        public async Task<IRpcClient> BuildAsync()
        {
            _configuration.RequestAddress = _addressBuilder.Address();
            _configuration.Connection = _connection;
            var rpcClient = new AmqpRpcClient(_configuration);
            await rpcClient.OpenAsync().ConfigureAwait(false);
            return rpcClient;
        }
    }

    public class AmqpRpcClient : AbstractLifeCycle, IRpcClient
    {
        private readonly RpcClientConfiguration _configuration;
        private IConsumer? _consumer = null;
        private IPublisher? _publisher = null;
        private readonly Dictionary<object, TaskCompletionSource<IMessage>> _pendingRequests = new();
        private readonly string _correlationId = Guid.NewGuid().ToString();
        private int _nextCorrelationId = 0;

        private object CorrelationIdSupplier()
        {
            if (_configuration.CorrelationIdSupplier != null)
            {
                return _configuration.CorrelationIdSupplier();
            }

            return $"{_correlationId}-" + Interlocked.Increment(ref _nextCorrelationId);
        }

        private object ExtractCorrelationId(IMessage message)
        {
            object corr = message.CorrelationId();
            if (_configuration.CorrelationIdExtractor != null)
            {
                corr = _configuration.CorrelationIdExtractor(message);
            }

            return corr;

        }
        public AmqpRpcClient(RpcClientConfiguration configuration)
        {
            _configuration = configuration;
        }

        public override async Task OpenAsync()
        {
            _publisher = await _configuration.Connection.PublisherBuilder().BuildAsync().ConfigureAwait(false);
            _consumer = await _configuration.Connection.ConsumerBuilder()
                .Queue(_configuration.ReplyToQueue)
                .MessageHandler(async (context, message) =>
                {
                    await context.AcceptAsync().ConfigureAwait(false);
                    object correlationId = ExtractCorrelationId(message);
                    if (_pendingRequests.TryGetValue(correlationId, out TaskCompletionSource<IMessage>? request))
                    {
                        request.SetResult(message);
                    }
                }).BuildAndStartAsync().ConfigureAwait(false);

            await base.OpenAsync().ConfigureAwait(false);
        }

        public override async Task CloseAsync()
        {
            OnNewStatus(State.Closing, null);
            try
            {
                if (_publisher != null)
                {
                    await _publisher.CloseAsync().ConfigureAwait(false);
                }

                if (_consumer != null)
                {
                    await _consumer.CloseAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                OnNewStatus(State.Closed, null);
            }
        }

        public async Task<IMessage> PublishAsync(IMessage message, CancellationToken cancellationToken = default)
        {
            message.MessageId(CorrelationIdSupplier());
            //TODO: use correlation id to match request and response
            _pendingRequests.Add(message.MessageId(), new TaskCompletionSource<IMessage>());
            if (_publisher != null)
            {
                PublishResult pr = await _publisher.PublishAsync(
                    message.ReplyTo(
                            new AddressBuilder().Queue(_configuration.ReplyToQueue).Address())
                        .To(_configuration.RequestAddress), cancellationToken).ConfigureAwait(false);

                if (pr.Outcome.State != OutcomeState.Accepted)
                {
                    _pendingRequests[message.CorrelationId()]
                        .SetException(new Exception($"Failed to send request state: {pr.Outcome.State}"));
                }
            }

            await _pendingRequests[message.MessageId()].Task.WaitAsync(_configuration.Timeout)
                .ConfigureAwait(false);

            return await _pendingRequests[message.MessageId()].Task.ConfigureAwait(false);
        }
    }
}

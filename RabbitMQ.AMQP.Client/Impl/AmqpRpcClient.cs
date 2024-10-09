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
    }

    public class AmqpRpcClientBuilder : IRpcClientBuilder
    {
        private readonly RpcClientAddressBuilder _addressBuilder;
        private AmqpConnection _connection;
        private RpcClientConfiguration _configuration = new();

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
        private readonly Dictionary<string, TaskCompletionSource<IMessage>> _pendingRequests = new();

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
                    if (_pendingRequests.ContainsKey(message.MessageId()))
                    {
                        _pendingRequests[message.MessageId()].SetResult(message);
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
            _pendingRequests.Add(message.MessageId(), new TaskCompletionSource<IMessage>());
            if (_publisher != null)
            {
                PublishResult pr = await _publisher.PublishAsync(
                    message.ReplyTo(new AddressBuilder().Queue(_configuration.ReplyToQueue).Address())
                        .To(_configuration.RequestAddress), cancellationToken).ConfigureAwait(false);

                if (pr.Outcome.State != OutcomeState.Accepted)
                {
                    _pendingRequests[message.MessageId()]
                        .SetException(new Exception($"Failed to send request state: {pr.Outcome.State}"));
                }
            }

            await _pendingRequests[message.MessageId()].Task.WaitAsync(_configuration.Timeout)
                .ConfigureAwait(false);

            return await _pendingRequests[message.MessageId()].Task.ConfigureAwait(false);
        }
    }
}

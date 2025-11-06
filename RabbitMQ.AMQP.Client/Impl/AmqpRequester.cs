// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client.Impl
{
    public class RequesterConfiguration
    {
        public AmqpConnection Connection { get; set; } = null!;
        public string ReplyToQueue { get; set; } = "";

        public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);

        public Func<object>? CorrelationIdSupplier { get; set; } = null;

        public Func<IMessage, object>? CorrelationIdExtractor { get; set; }

        public Func<IMessage, object, IMessage>? RequestPostProcessor { get; set; }
    }

    public class AmqpRequesterBuilder : IRequesterBuilder
    {
        private readonly RequesterAddressBuilder _addressBuilder;
        private readonly AmqpConnection _connection;
        private readonly RequesterConfiguration _configuration = new();

        public AmqpRequesterBuilder(AmqpConnection connection)
        {
            _connection = connection;
            _addressBuilder = new RequesterAddressBuilder(this);
        }

        public IRequesterAddressBuilder RequestAddress()
        {
            return _addressBuilder;
        }

        public IRequesterBuilder ReplyToQueue(string replyToQueueName)
        {
            _configuration.ReplyToQueue = replyToQueueName;
            return this;
        }

        public IRequesterBuilder ReplyToQueue(IQueueSpecification replyToQueue)
        {
            _configuration.ReplyToQueue = replyToQueue.QueueName;
            return this;
        }

        public IRequesterBuilder CorrelationIdExtractor(Func<IMessage, object>? correlationIdExtractor)
        {
            _configuration.CorrelationIdExtractor = correlationIdExtractor;
            return this;
        }

        public IRequesterBuilder RequestPostProcessor(Func<IMessage, object, IMessage>? requestPostProcessor)
        {
            _configuration.RequestPostProcessor = requestPostProcessor;
            return this;
        }

        public IRequesterBuilder CorrelationIdSupplier(Func<object>? correlationIdSupplier)
        {
            _configuration.CorrelationIdSupplier = correlationIdSupplier;
            return this;
        }

        public IRequesterBuilder Timeout(TimeSpan timeout)
        {
            _configuration.Timeout = timeout;
            return this;
        }

        public async Task<IRequester> BuildAsync()
        {
            _configuration.Connection = _connection;
            var rpcClient = new AmqpRequester(_configuration);
            await rpcClient.OpenAsync().ConfigureAwait(false);
            return rpcClient;
        }
    }

    /// <summary>
    /// AmqpRpcClient is an implementation of <see cref="IRequester"/>.
    /// It is a wrapper around <see cref="IPublisher"/> and <see cref="IConsumer"/> to create an RPC client over AMQP 1.0.
    /// even the PublishAsync is async the RPClient blocks the thread until the response is received.
    /// within the timeout.
    ///
    ///  The PublishAsync is thread-safe and can be called from multiple threads.
    ///
    /// See also the server side <see cref="IResponder"/>.
    /// </summary>
    public class AmqpRequester : AbstractLifeCycle, IRequester
    {
        private readonly RequesterConfiguration _configuration;
        private IConsumer? _consumer = null;
        private IPublisher? _publisher = null;
        private readonly ConcurrentDictionary<object, TaskCompletionSource<IMessage>> _pendingRequests = new();
        private readonly Guid _correlationId = Guid.NewGuid();
        private readonly SemaphoreSlim _semaphore = new(1, 1);
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

        private IMessage RequestPostProcess(IMessage request, object correlationId)
        {
            if (_configuration.RequestPostProcessor != null)
            {
                return _configuration.RequestPostProcessor(request, correlationId);
            }

            string s = GetReplyToQueue();
            return request.ReplyTo(AddressBuilderHelper.AddressBuilder().Queue(s).Address())
                .MessageId(correlationId);
        }

        public AmqpRequester(RequesterConfiguration configuration)
        {
            _configuration = configuration;
        }

        /// <summary>
        /// OpenAsync initializes the Requester by creating the necessary publisher and consumer.
        /// The DirectReplyTo feature is applied if supported by the server and no explicit reply-to queue is set.
        /// The AmqpRequester is an opinionated wrapper to simulate RPC over AMQP 1.0.
        /// Even when the DirectReplyTo is supported the wrapper can decide to don't use it when
        /// the user has explicitly set a reply-to queue.
        /// </summary>
        /// <returns></returns>
        public override async Task OpenAsync()
        {
            bool isDirectReplyToSupported = _configuration.Connection._featureFlags.IsDirectReplyToSupported;

            string queueReplyTo = _configuration.ReplyToQueue;
            // if the queue is not set it means we need to create a temporary queue as reply-to
            // only if direct-reply-to is not supported. In case of isDirectReplyToSupported the server 
            // will create the server side temporary queue.
            if (string.IsNullOrEmpty(_configuration.ReplyToQueue) && !isDirectReplyToSupported)
            {
                IQueueInfo queueInfo = await _configuration.Connection.Management().Queue().AutoDelete(true)
                    .Exclusive(true).DeclareAsync()
                    .ConfigureAwait(false);
                queueReplyTo = queueInfo.Name();
            }

            // we can apply DirectReplyTo only if the _configuration.ReplyToQueue is not set 
            // and the server support DirectReplyTo feature
            // AmqpRequester is a wrapper to simulate RPC over AMQP 1.0
            // canApplyDirectReplyTo is an opinionated optimization to avoid creating temporary queues
            // unless _configuration.ReplyToQueue is explicitly set by the user.
            // The user is always free to create custom Requester and Responder 
            bool canApplyDirectReplyTo = isDirectReplyToSupported &&
                                         string.IsNullOrEmpty(_configuration.ReplyToQueue);

            _publisher = await _configuration.Connection.PublisherBuilder().BuildAsync().ConfigureAwait(false);
            _consumer = await _configuration.Connection.ConsumerBuilder()
                .Queue(queueReplyTo)
                .DirectReplyTo(canApplyDirectReplyTo)
                .MessageHandler((context, message) =>
                {
                    // TODO MessageHandler funcs should catch all exceptions
                    context.Accept();
                    object correlationId = ExtractCorrelationId(message);
                    if (_pendingRequests.TryGetValue(correlationId, out TaskCompletionSource<IMessage>? request))
                    {
                        request.SetResult(message);
                    }

                    return Task.CompletedTask;
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
            await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                object correlationId = CorrelationIdSupplier();
                message = RequestPostProcess(message, correlationId);
                _pendingRequests.TryAdd(correlationId, Utils.CreateTaskCompletionSource<IMessage>());
                if (_publisher != null)
                {
                    PublishResult pr = await _publisher.PublishAsync(
                        message.To(AddressBuilderHelper.AddressBuilder().Queue(GetReplyToQueue()).Address()),
                        cancellationToken).ConfigureAwait(false);

                    if (pr.Outcome.State != OutcomeState.Accepted)
                    {
                        _pendingRequests[correlationId]
                            .SetException(new Exception($"Failed to send request state: {pr.Outcome.State}"));
                    }
                }

                await _pendingRequests[correlationId].Task.WaitAsync(_configuration.Timeout)
                    .ConfigureAwait(false);

                return await _pendingRequests[correlationId].Task.ConfigureAwait(false);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public string GetReplyToQueue()
        {
            if (_consumer == null)
            {
                throw new InvalidOperationException("Requester is not opened");
            }

            return _consumer.Queue ??
                   throw new InvalidOperationException("ReplyToQueueAddress is not available");
        }
    }
}

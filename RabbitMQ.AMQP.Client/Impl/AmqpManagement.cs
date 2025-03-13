// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

namespace RabbitMQ.AMQP.Client.Impl
{
    /// <summary>
    /// AmqpManagement implements the <see cref="IManagement"/> interface
    /// to manage the <see href="https://www.rabbitmq.com/tutorials/amqp-concepts">AMQP 0.9.1 model</see> 
    /// topology (exchanges, queues, and bindings).
    /// </summary>
    public class AmqpManagement : AbstractLifeCycle, IManagement, IManagementTopology
    {
        private readonly AmqpManagementParameters _amqpManagementParameters;

        // The requests are stored in a dictionary with the correlationId as the key
        // The correlationId is used to match the request with the response
        private readonly ConcurrentDictionary<string, TaskCompletionSource<Message>> _requests = new();

        // private static readonly long IdSequence = 0;
        private const string ManagementNodeAddress = "/management";
        private const string LinkPairName = "management-link-pair";

        private Session? _managementSession;
        private SenderLink? _senderLink;
        private ReceiverLink? _receiverLink;

        internal const int Code200 = 200;
        internal const int Code201 = 201;
        internal const int Code204 = 204;
        internal const int Code409 = 409;
        internal const int Code400 = 400;
        internal const string Put = "PUT";
        internal const string Get = "GET";
        internal const string Post = "POST";
        internal const string Delete = "DELETE";
        private const string ReplyTo = "$me";
        private const string AuthTokens = "/auth/tokens";

        protected readonly TaskCompletionSource<bool> _managementSessionClosedTcs =
            Utils.CreateTaskCompletionSource<bool>();

        internal AmqpManagement(AmqpManagementParameters amqpManagementParameters)
        {
            _amqpManagementParameters = amqpManagementParameters;
        }

        /// <summary>
        /// Create an <see cref="IQueueSpecification"/>, with an auto-generated name.
        /// </summary>
        /// <returns>A builder for <see cref="IQueueSpecification"/></returns>
        public IQueueSpecification Queue()
        {
            ThrowIfClosed();
            return new AmqpQueueSpecification(this);
        }

        /// <summary>
        /// Create an <see cref="IQueueSpecification"/>, with the given name.
        /// </summary>
        /// <returns>A builder for <see cref="IQueueSpecification"/></returns>
        public IQueueSpecification Queue(string name)
        {
            return Queue().Name(name);
        }

        /// <summary>
        /// Get the <see cref="IQueueInfo"/> for the given queue specification
        /// </summary>
        /// <param name="queueSpec">The <see cref="IQueueSpecification"/></param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/></param>
        /// <returns>The <see cref="IQueueInfo"/> for the given spec.</returns>
        public Task<IQueueInfo> GetQueueInfoAsync(IQueueSpecification queueSpec,
            CancellationToken cancellationToken = default)
        {
            return GetQueueInfoAsync(queueSpec.QueueName, cancellationToken);
        }

        /// <summary>
        /// Get the <see cref="IQueueInfo"/> for the given queue name.
        /// </summary>
        /// <param name="queueName">The queue name</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/></param>
        /// <returns>The <see cref="IQueueInfo"/> for the given spec.</returns>
        public async Task<IQueueInfo> GetQueueInfoAsync(string queueName,
            CancellationToken cancellationToken = default)
        {
            // TODO: validate queueName?
            // TODO: encodePathSegment(queues)
            string path = $"/{Consts.Queues}/{Utils.EncodePathSegment(queueName)}";
            string method = AmqpManagement.Get;
            int[] expectedResponseCodes = new int[] { AmqpManagement.Code200 };
            Message response = await RequestAsync(path, method, expectedResponseCodes, null, cancellationToken)
                .ConfigureAwait(false);

            return new DefaultQueueInfo((Map)response.Body);
        }

        /// <summary>
        /// Create an <see cref="IExchangeSpecification"/>, with an auto-generated name.
        /// </summary>
        /// <returns>A builder for <see cref="IExchangeSpecification"/></returns>
        public IExchangeSpecification Exchange()
        {
            ThrowIfClosed();
            return new AmqpExchangeSpecification(this);
        }

        /// <summary>
        /// Create an <see cref="IExchangeSpecification"/>, with the given name.
        /// </summary>
        /// <returns>A builder for <see cref="IExchangeSpecification"/></returns>
        public IExchangeSpecification Exchange(string name)
        {
            return Exchange().Name(name);
        }

        /// <summary>
        /// Create an <see cref="IBindingSpecification"/>.
        /// </summary>
        /// <returns>A builder for <see cref="IBindingSpecification"/></returns>
        public IBindingSpecification Binding()
        {
            return new AmqpBindingSpecification(this);
        }

        public async Task RefreshTokenAsync(string token)
        {
            int[] expectedResponseCodes = { Code204 };
            _ = await RequestAsync( Encoding.ASCII.GetBytes(token),
            AuthTokens, Put, expectedResponseCodes)
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Open the management session.
        /// </summary>
        public override async Task OpenAsync()
        {
            if (State == State.Open)
            {
                return;
            }

            if (_managementSession == null || _managementSession.IsClosed)
            {
                _managementSession = new Session(_amqpManagementParameters.NativeConnection);
            }

            await EnsureSenderLinkAsync()
                .ConfigureAwait(false);

            await EnsureReceiverLinkAsync()
                .ConfigureAwait(false);

            // TODO do something with this task?
            _ = Task.Run(ProcessResponses);

            _managementSession.AddClosedCallback(OnManagementSessionClosed);

            await base.OpenAsync()
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Close the management session.
        /// </summary>
        public override async Task CloseAsync()
        {
            // TODO 10 seconds seems too long
            TimeSpan closeSpan = TimeSpan.FromSeconds(10);

            if (_managementSession is { IsClosed: false })
            {
                OnNewStatus(State.Closing, null);

                await _managementSession.CloseAsync(closeSpan)
                    .ConfigureAwait(false);

                await _managementSessionClosedTcs.Task.WaitAsync(closeSpan)
                    .ConfigureAwait(false);

                _managementSession = null;
                _senderLink = null;
                _receiverLink = null;

                // this is actually a double set of the status, but it is needed to ensure that the status is set to closed
                // but the `OnNewStatus` is idempotent
                OnNewStatus(State.Closed, null);
            }
        }

        /// <summary>
        /// Convert this <see cref="AmqpManagement"/> instance to a string.
        /// </summary>
        /// <returns>The string representation of this <see cref="AmqpManagement"/> instance.</returns>
        public override string ToString()
        {
            string info = $"AmqpManagement{{" +
                          $"AmqpConnection='{_amqpManagementParameters.Connection}', " +
                          $"Status='{State.ToString()}'" +
                          $"ReceiverLink closed: {_receiverLink?.IsClosed} " +
                          $"}}";

            return info;
        }

        ITopologyListener IManagementTopology.TopologyListener()
        {
            return _amqpManagementParameters.TopologyListener();
        }

        internal protected virtual Task InternalSendAsync(Message message, TimeSpan timeout)
        {
            if (_senderLink is null)
            {
                // TODO create "internal bug" exception type?
                throw new InvalidOperationException(
                    "_senderLink is null, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
            }

            return _senderLink.SendAsync(message, timeout);
        }

        internal protected void HandleResponseMessage(Message msg)
        {
            if (msg.Properties.CorrelationId != null &&
                _requests.TryRemove(msg.Properties.CorrelationId, out TaskCompletionSource<Message>? mre))
            {
                if (mre.TrySetResult(msg))
                {
                    Trace.WriteLine(TraceLevel.Verbose, $"Set result for:  {msg.Properties.CorrelationId}");
                }
            }
            else
            {
                Trace.WriteLine(TraceLevel.Error, $"No request found for message: {msg.Properties.CorrelationId}");
            }
        }

        internal IQueueSpecification Queue(QueueSpec spec)
        {
            return Queue().Name(spec.QueueName)
                .AutoDelete(spec.IsAutoDelete)
                .Exclusive(spec.IsExclusive)
                .Arguments(spec.QueueArguments);
        }

        internal IExchangeSpecification Exchange(ExchangeSpec spec)
        {
            return Exchange().Name(spec.ExchangeName)
                .AutoDelete(spec.IsAutoDelete)
                .Type(spec.ExchangeType)
                .Arguments(spec.ExchangeArguments);
        }

        internal IBindingSpecification Binding(BindingSpec spec)
        {
            return Binding()
                .SourceExchange(spec.SourceExchangeName)
                .DestinationQueue(spec.DestinationQueueName)
                .DestinationExchange(spec.DestinationExchangeName)
                .Key(spec.BindingKey)
                .Arguments(spec.BindingArguments);
        }

        internal Task<Message> RequestAsync(string path, string method,
            int[] expectedResponseCodes,
            TimeSpan? timeout = null,
            CancellationToken cancellationToken = default)
        {
            return RequestAsync(null, path, method, expectedResponseCodes,
                timeout, cancellationToken);
        }

        internal Task<Message> RequestAsync(object? body, string path, string method,
            int[] expectedResponseCodes,
            TimeSpan? timeout = null, // TODO no need for timeouts with CancellationToken
            CancellationToken cancellationToken = default)
        {
            string id = Guid.NewGuid().ToString();
            return RequestAsync(id, body, path, method, expectedResponseCodes, timeout, cancellationToken);
        }

        internal Task<Message> RequestAsync(string id, object? body, string path, string method,
            int[] expectedResponseCodes,
            TimeSpan? timeout = null,
            CancellationToken cancellationToken = default)
        {
            var message = new Message(body)
            {
                Properties = new Properties { MessageId = id, To = path, Subject = method, ReplyTo = ReplyTo }
            };

            return RequestAsync(message, expectedResponseCodes, timeout, cancellationToken);
        }

        /// <summary>
        /// Core function to send a request and wait for the response
        /// The request is an AMQP message with the following properties:
        /// - Properties.MessageId: Mandatory to identify the request
        /// - Properties.To: The path of the request, for example "/queues/my-queue"
        /// - Properties.Subject: The method of the request, for example "PUT"
        /// - Properties.ReplyTo: The address where the response will be sent. Default is: "$me"
        /// - Body: The body of the request. For example the QueueSpec to create a queue
        /// </summary>
        /// <param name="message">Request Message. Contains all the info to create/delete a resource</param>
        /// <param name="expectedResponseCodes">The response codes expected for a specific call. See Code* Constants </param>
        /// <param name="argTimeout">Default timeout for a request </param>
        /// <param name="cancellationToken">Cancellation token for this request</param>
        /// <returns> A message with the Info response. For example in case of Queue creation is DefaultQueueInfo </returns>
        /// <exception cref="ModelException"> Application errors, see <see cref="ModelException"/> </exception>
        internal async Task<Message> RequestAsync(Message message, int[] expectedResponseCodes,
            TimeSpan? argTimeout = null, CancellationToken cancellationToken = default)
        {
            ThrowIfClosed();

            // TODO: no need for timeout when there is a CancellationToken
            TimeSpan timeout = argTimeout ?? TimeSpan.FromSeconds(30);

            TaskCompletionSource<Message> tcs = Utils.CreateTaskCompletionSource<Message>();

            // Add TaskCompletionSource to the dictionary it will be used to set the result of the request
            if (false == _requests.TryAdd(message.Properties.MessageId, tcs))
            {
                // TODO what to do in this error case?
            }

            // TODO: re-use with TryReset?
            using var timeoutCts = new CancellationTokenSource(timeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

            void RequestTimeoutAction()
            {
                Trace.WriteLine(TraceLevel.Warning, $"Request timeout for {message.Properties.MessageId}");

                if (_requests.TryRemove(message.Properties.MessageId, out TaskCompletionSource<Message>? timedOutMre))
                {
                    if (false == timedOutMre.TrySetCanceled(linkedCts.Token))
                    {
                        // TODO debug log rare condition?
                    }
                }
                else
                {
                    // TODO log missing request?
                }
            }

            using CancellationTokenRegistration ctsr = timeoutCts.Token.Register(RequestTimeoutAction);

            // NOTE: no cancellation token support
            Task sendTask = InternalSendAsync(message, timeout);

            // The response is handled in a separate thread, see ProcessResponses method in the Init method
            // TODO timeout & token
            Message result = await tcs.Task.WaitAsync(linkedCts.Token)
                .ConfigureAwait(false);

            await sendTask.WaitAsync(linkedCts.Token)
                .ConfigureAwait(false);

            // Check the responses and throw exceptions if needed.
            CheckResponse(message, expectedResponseCodes, result);

            return result;
        }

        internal void ChangeStatus(State newState, Error? error)
        {
            OnNewStatus(newState, error);
        }

        /// <summary>
        /// Check the response of a request and throw exceptions if needed
        /// </summary>
        /// <param name="sentMessage">The message sent </param>
        /// <param name="expectedResponseCodes"> The expected response codes  </param>
        /// <param name="receivedMessage">The message received from the server</param>
        /// <exception cref="ModelException"></exception>
        /// <exception cref="PreconditionFailedException"></exception>
        /// <exception cref="InvalidCodeException"></exception>
        internal void CheckResponse(Message sentMessage, int[] expectedResponseCodes, Message receivedMessage)
        {
            // Check if the response code is a number
            // by protocol the response code is in the Subject property
            if (!int.TryParse(receivedMessage.Properties.Subject, out int responseCode))
            {
                throw new ModelException($"Response code is not a number {receivedMessage.Properties.Subject}");
            }

            switch (responseCode)
            {
                case Code409:
                    throw new PreconditionFailedException($"{receivedMessage.Body}, response code: {responseCode}");
                case Code400:
                    throw new BadRequestException($"{receivedMessage.Body}, response code: {responseCode}");
            }

            // Check if the correlationId is the same as the messageId
            if (sentMessage.Properties.MessageId != receivedMessage.Properties.CorrelationId)
            {
                throw new ModelException(
                    $"CorrelationId does not match, expected {sentMessage.Properties.MessageId} but got {receivedMessage.Properties.CorrelationId}");
            }

            bool any = expectedResponseCodes.Any(c => c == responseCode);
            if (!any)
            {
                throw new InvalidCodeException(
                    $"Unexpected response code: {responseCode} instead of {expectedResponseCodes.Aggregate("", (s, i) => s + i + ", ")}"
                );
            }
        }

        private void OnManagementSessionClosed(IAmqpObject sender, Amqp.Framing.Error error)
        {
            if (State != State.Closed && error != null)
            {
                Trace.WriteLine(TraceLevel.Warning, $"Management session closed " +
                                                    $"with error: {Utils.ConvertError(error)} " +
                                                    $" AmqpManagement: {ToString()}");
            }

            OnNewStatus(State.Closed, Utils.ConvertError(error));

            // Note: TrySetResult *must* be used here
            _managementSessionClosedTcs.TrySetResult(true);
        }

        private async Task ProcessResponses()
        {
            try
            {
                while (_managementSession?.IsClosed == false &&
                       _amqpManagementParameters.IsNativeConnectionClosed == false)
                {
                    if (_receiverLink == null)
                    {
                        continue;
                    }

                    TimeSpan timeout = TimeSpan.FromSeconds(59);
                    using (Message msg = await _receiverLink.ReceiveAsync(timeout).ConfigureAwait(false))
                    {
                        if (msg == null)
                        {
                            // this is not a problem, it is just a timeout. 
                            // the timeout is set to 60 seconds. 
                            // For the moment I'd trace it at some point we can remove it
                            Trace.WriteLine(TraceLevel.Verbose,
                                $"{ToString()} - Timeout {timeout.Seconds} s.. waiting for message.");
                            continue;
                        }

                        _receiverLink.Accept(msg);
                        HandleResponseMessage(msg);
                    }
                }
            }
            catch (Exception e)
            {
                // TODO this is a serious situation that should be thrown
                // up to the client application
                if (_receiverLink?.IsClosed == false)
                {
                    Trace.WriteLine(TraceLevel.Error,
                        $"Receiver link error in management session {e}. Receiver link closed: {_receiverLink?.IsClosed}");
                }
            }

            Trace.WriteLine(TraceLevel.Verbose, "ProcessResponses Task closed");
        }

        private async Task EnsureReceiverLinkAsync()
        {
            if (_receiverLink == null || _receiverLink.IsClosed)
            {
                var receiveAttach = new Attach()
                {
                    SndSettleMode = SenderSettleMode.Settled,
                    RcvSettleMode = ReceiverSettleMode.First,
                    Properties = new Fields { { new Symbol("paired"), true } },
                    LinkName = LinkPairName,
                    Source =
                        new Source() { Address = ManagementNodeAddress, ExpiryPolicy = new Symbol("LINK_DETACH"), },
                    Handle = 1,
                    Target =
                        new Target() { Address = ManagementNodeAddress, ExpiryPolicy = new Symbol("SESSION_END"), },
                };

                TaskCompletionSource<ReceiverLink> tcs = Utils.CreateTaskCompletionSource<ReceiverLink>();
                var tmpReceiverLink = new ReceiverLink(
                    _managementSession, LinkPairName, receiveAttach, (ILink link, Attach attach) =>
                    {
                        if (link is ReceiverLink receiverLink)
                        {
                            tcs.SetResult(receiverLink);
                        }
                        else
                        {
                            // TODO create "internal bug" exception type?
                            var ex = new InvalidOperationException(
                                "invalid link in OnAttached, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
                            tcs.SetException(ex);
                        }
                    });

                _receiverLink = await tcs.Task
                    .ConfigureAwait(false);

                if (false == Object.ReferenceEquals(_receiverLink, tmpReceiverLink))
                {
                    // TODO log this case?
                }

                // TODO
                // using a credit of 1 can result in AmqpExceptions in ProcessResponses
                _receiverLink.SetCredit(100);
            }
        }

        private async Task EnsureSenderLinkAsync()
        {
            if (_senderLink == null || _senderLink.IsClosed)
            {
                var senderAttach = new Attach
                {
                    SndSettleMode = SenderSettleMode.Settled,
                    RcvSettleMode = ReceiverSettleMode.First,
                    Properties = new Fields { { new Symbol("paired"), true } },
                    LinkName = LinkPairName,
                    Source = new Source()
                    {
                        Address = ManagementNodeAddress,
                        ExpiryPolicy = new Symbol("LINK_DETACH"),
                        Timeout = 0,
                        Dynamic = false,
                        Durable = 0
                    },
                    Handle = 0,
                    Target = new Target()
                    {
                        Address = ManagementNodeAddress,
                        ExpiryPolicy = new Symbol("SESSION_END"),
                        Timeout = 0,
                        Dynamic = false,
                    },
                };

                TaskCompletionSource<SenderLink> tcs = Utils.CreateTaskCompletionSource<SenderLink>();
                var tmpSenderLink = new SenderLink(
                    _managementSession, LinkPairName, senderAttach, (ILink link, Attach attach) =>
                    {
                        if (link is SenderLink senderLink)
                        {
                            tcs.SetResult(senderLink);
                        }
                        else
                        {
                            // TODO create "internal bug" exception type?
                            var ex = new InvalidOperationException(
                                "invalid link in OnAttached, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
                            tcs.SetException(ex);
                        }
                    });

                _senderLink = await tcs.Task
                    .ConfigureAwait(false);

                if (false == Object.ReferenceEquals(_senderLink, tmpSenderLink))
                {
                    // TODO log this case?
                }
            }
        }
    }
}

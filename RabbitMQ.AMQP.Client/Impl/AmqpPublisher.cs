// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

namespace RabbitMQ.AMQP.Client.Impl
{
    /// <summary>
    /// Implementation of <see cref="IPublisher"/>.
    /// </summary>
    public class AmqpPublisher : AbstractReconnectLifeCycle, IPublisher
    {
        private readonly AmqpConnection _connection;
        private readonly string? _address;
        private readonly IMetricsReporter? _metricsReporter;
        private readonly Guid _id = Guid.NewGuid();

        private SenderLink? _senderLink = null;

        /// <summary>
        /// Test seam: when set, used instead of <see cref="SenderLink.Send(Message, OutcomeCallback, object)"/>.
        /// </summary>
        internal Action<Message, OutcomeCallback, object>? SendOverride { get; set; }

        public AmqpPublisher(AmqpConnection connection, string? address, IMetricsReporter? metricsReporter)
        {
            _connection = connection;
            _address = address;
            _metricsReporter = metricsReporter;
            _connection.AddPublisher(_id, this);
        }

        /// <summary>
        /// Open this publisher
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the async operation.</returns>
        public override async Task OpenAsync()
        {
            try
            {
                TaskCompletionSource<SenderLink> attachCompletedTcs =
                    Utils.CreateTaskCompletionSource<SenderLink>();

                Attach attach = Utils.CreateAttach(_address, DeliveryMode.AtLeastOnce, _id);

                void OnAttached(ILink argLink, Attach argAttach)
                {
                    if (argLink is SenderLink link)
                    {
                        attachCompletedTcs.SetResult(link);
                    }
                    else
                    {
                        // TODO create "internal bug" exception type?
                        var ex = new InvalidOperationException(
                            "invalid link in onAttached, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
                        attachCompletedTcs.SetException(ex);
                    }
                }

                Session session = await _connection._nativePubSubSessions.GetOrCreateSessionAsync()
                    .ConfigureAwait(false);
                var tmpSenderLink = new SenderLink(session, _id.ToString(), attach, OnAttached);

                // TODO configurable timeout
                var waitSpan = TimeSpan.FromSeconds(5);
                // TODO
                // Even 10ms is enough to allow the links to establish,
                // which tells me it allows the .NET runtime to process
                await Task.Delay(10)
                    .ConfigureAwait(false);

                _senderLink = await attachCompletedTcs.Task.WaitAsync(waitSpan)
                    .ConfigureAwait(false);

                if (false == Object.ReferenceEquals(_senderLink, tmpSenderLink))
                {
                    // TODO log this case?
                }

                if (_senderLink is null)
                {
                    throw new PublisherException($"{ToString()} Failed to create sender link (null was returned)");
                }
                else if (_senderLink.LinkState != LinkState.Attached)
                {
                    throw new PublisherException(
                        $"{ToString()} Failed to create sender link. Link state is not attached, error: " +
                        _senderLink.Error?.ToString() ?? "Unknown error");
                }
                else
                {
                    await base.OpenAsync()
                        .ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                throw new PublisherException($"{ToString()} Failed to create sender link, {e}");
            }
        }

        public Task<PublishResult> PublishAsync(IMessage message, CancellationToken cancellationToken = default)
        {
            ThrowIfClosed();

            if (_senderLink is null && SendOverride is null)
            {
                // TODO create "internal bug" exception type?
                throw new InvalidOperationException(
                    "_senderLink is null, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
            }

            Stopwatch? stopwatch = null;
            if (_metricsReporter is not null)
            {
                stopwatch = new();
                stopwatch.Start();
            }

            TaskCompletionSource<PublishResult> publishResultTcs =
                Utils.CreateTaskCompletionSource<PublishResult>();

            Message nativeMessage = ((AmqpMessage)message).NativeMessage;

            void OutcomeCallback(ILink sender, Message inMessage, Outcome outcome, object state)
            {
                // Note: sometimes `inMessage` is null 🤔
                Debug.Assert(ReferenceEquals(this, state));

                if (!ReferenceEquals(_senderLink, sender))
                {
                    // TODO log this case?
                }

                PublishOutcome publishOutcome;
                IMetricsReporter.PublishDispositionValue dispositionValue;
                switch (outcome)
                {
                    case Rejected rejectedOutcome:
                        {
                            const OutcomeState publishState = OutcomeState.Rejected;
                            publishOutcome = new PublishOutcome(publishState,
                                Utils.ConvertError(rejectedOutcome.Error),
                                Utils.ConvertToAmqpMessageRejectedException(rejectedOutcome.Error));
                            dispositionValue = IMetricsReporter.PublishDispositionValue.REJECTED;
                            break;
                        }
                    case Released:
                        {
                            const OutcomeState publishState = OutcomeState.Released;
                            publishOutcome = new PublishOutcome(publishState, null);
                            dispositionValue = IMetricsReporter.PublishDispositionValue.RELEASED;
                            break;
                        }
                    case Accepted:
                        {
                            const OutcomeState publishState = OutcomeState.Accepted;
                            publishOutcome = new PublishOutcome(publishState, null);
                            dispositionValue = IMetricsReporter.PublishDispositionValue.ACCEPTED;
                            break;
                        }
                    default:
                        {
                            throw new NotSupportedException();
                        }
                }

                var publishResult = new PublishResult(message, publishOutcome);

                // The first completion wins and gates all metrics, so the counters
                // always describe the outcome the returned task carries.
                if (publishResultTcs.TrySetResult(publishResult))
                {
                    _metricsReporter?.PublishDisposition(dispositionValue);

                    // TODO cancellation token
                    if (_metricsReporter is not null && stopwatch is not null)
                    {
                        stopwatch.Stop();
                        _metricsReporter.Published(stopwatch.Elapsed);
                    }
                }
            }

            /*
             * Note: do NOT use SendAsync here as it prevents the Closed event from
             * firing on the native connection. Bizarre, I know!
             */
            try
            {
                if (SendOverride is not null)
                {
                    SendOverride(nativeMessage, OutcomeCallback, this);
                }
                else
                {
                    _senderLink!.Send(nativeMessage, OutcomeCallback, this);
                }
            }
            catch (AmqpException amqpException)
            {
                var publishOutcome = new PublishOutcome(OutcomeState.Rejected, Utils.ConvertError(amqpException.Error));
                var publishResult = new PublishResult(message, publishOutcome);
                if (publishResultTcs.TrySetResult(publishResult))
                {
                    stopwatch?.Stop();
                    _metricsReporter?.PublishDisposition(IMetricsReporter.PublishDispositionValue.REJECTED);
                }
            }
            catch (Exception ex)
            {
                var publisherException = new PublisherException($"{ToString()} Failed to publish message", ex);
                publishResultTcs.TrySetException(publisherException);
            }

            return publishResultTcs.Task;
        }

        /// <summary>
        /// Close this publisher
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the async operation.</returns>
        public override async Task CloseAsync()
        {
            if (_senderLink is null)
            {
                return;
            }

            OnNewStatus(State.Closing, null);

            try
            {
                // TODO global timeout for closing, other async actions?
                await _senderLink.CloseAsync(TimeSpan.FromSeconds(5))
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Trace.WriteLine(TraceLevel.Warning, "Failed to close sender link. The publisher will be closed anyway",
                    ex);
            }

            _senderLink = null;
            OnNewStatus(State.Closed, null);
            _connection.RemovePublisher(_id);
        }

        public override string ToString()
        {
            return $"Publisher{{Address='{_address}', id={_id} Connection='{_connection}', State='{State}'}}";
        }
    }
}

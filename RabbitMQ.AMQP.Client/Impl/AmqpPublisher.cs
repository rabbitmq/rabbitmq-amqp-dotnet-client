﻿// This source code is dual-licensed under the Apache License, version 2.0,
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
    public class AmqpPublisher : AbstractReconnectLifeCycle, IPublisher
    {
        private readonly AmqpConnection _connection;
        private readonly string? _address;
        private readonly IMetricsReporter? _metricsReporter;
        private readonly Guid _id = Guid.NewGuid();

        private SenderLink? _senderLink = null;

        public AmqpPublisher(AmqpConnection connection, string? address, IMetricsReporter? metricsReporter)
        {
            _connection = connection;
            _address = address;
            _metricsReporter = metricsReporter;
            _connection.AddPublisher(_id, this);
        }

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

        /// <summary>
        /// Publishes a message to the broker in an asynchronous manner.
        /// The PublishResult is synchronous. In order to increase the performance
        /// you can use more tasks to publish messages in parallel
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        /// <exception cref="NotSupportedException"></exception>
        /// <exception cref="PublisherException"></exception>
        public async Task<PublishResult> PublishAsync(IMessage message, CancellationToken cancellationToken = default)
        {
            ThrowIfClosed();

            if (_senderLink is null)
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

            try
            {
                TaskCompletionSource<PublishOutcome> messagePublishedTcs =
                    Utils.CreateTaskCompletionSource<PublishOutcome>();

                Message nativeMessage = ((AmqpMessage)message).NativeMessage;

                void OutcomeCallback(ILink sender, Message inMessage, Outcome outcome, object state)
                {
                    // Note: sometimes `message` is null 🤔
                    System.Diagnostics.Debug.Assert(Object.ReferenceEquals(this, state));

                    if (false == Object.ReferenceEquals(_senderLink, sender))
                    {
                        // TODO log this case?
                    }

                    PublishOutcome publishOutcome;
                    switch (outcome)
                    {
                        case Rejected rejectedOutcome:
                            {
                                const OutcomeState publishState = OutcomeState.Rejected;
                                publishOutcome = new PublishOutcome(publishState,
                                    Utils.ConvertError(rejectedOutcome.Error));
                                _metricsReporter?.PublishDisposition(IMetricsReporter.PublishDispositionValue.REJECTED);
                                break;
                            }
                        case Released:
                            {
                                const OutcomeState publishState = OutcomeState.Released;
                                publishOutcome = new PublishOutcome(publishState, null);
                                _metricsReporter?.PublishDisposition(IMetricsReporter.PublishDispositionValue.RELEASED);
                                break;
                            }
                        case Accepted:
                            {
                                const OutcomeState publishState = OutcomeState.Accepted;
                                publishOutcome = new PublishOutcome(publishState, null);
                                _metricsReporter?.PublishDisposition(IMetricsReporter.PublishDispositionValue.ACCEPTED);
                                break;
                            }
                        default:
                            {
                                throw new NotSupportedException();
                            }
                    }

                    messagePublishedTcs.SetResult(publishOutcome);
                }

                /*
                 * Note: do NOT use SendAsync here as it prevents the Closed event from
                 * firing on the native connection. Bizarre, I know!
                 */
                _senderLink.Send(nativeMessage, OutcomeCallback, this);

                // TODO cancellation token
                // PublishOutcome publishOutcome = await messagePublishedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken)
                PublishOutcome publishOutcome = await messagePublishedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5))
                    .ConfigureAwait(false);

                if (_metricsReporter is not null && stopwatch is not null)
                {
                    stopwatch.Stop();
                    _metricsReporter.Published(stopwatch.Elapsed);
                }

                return new PublishResult(message, publishOutcome);
            }
            catch (AmqpException ex)
            {
                stopwatch?.Stop();
                _metricsReporter?.PublishDisposition(IMetricsReporter.PublishDispositionValue.REJECTED);
                var publishOutcome = new PublishOutcome(OutcomeState.Rejected, Utils.ConvertError(ex.Error));
                return new PublishResult(message, publishOutcome);
            }
            catch (Exception e)
            {
                throw new PublisherException($"{ToString()} Failed to publish message, {e}");
            }
        }

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

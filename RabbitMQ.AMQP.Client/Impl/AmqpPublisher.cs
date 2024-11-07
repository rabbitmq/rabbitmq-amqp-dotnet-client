// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
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
        private readonly TimeSpan _timeout;
        private readonly string? _address;
        private readonly Guid _id = Guid.NewGuid();

        private SenderLink? _senderLink = null;

        public AmqpPublisher(AmqpConnection connection, string? address, TimeSpan timeout)
        {
            _connection = connection;
            _address = address;
            _timeout = timeout;
            _connection.AddPublisher(_id, this);
        }

        public override async Task OpenAsync()
        {
            try
            {
                TaskCompletionSource<SenderLink> attachCompletedTcs =
                    new(TaskCreationOptions.RunContinuationsAsynchronously);

                Attach attach = Utils.CreateAttach(_address, DeliveryMode.AtLeastOnce, _id);

                void onAttached(ILink argLink, Attach argAttach)
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

                SenderLink? tmpSenderLink = null;
                Task senderLinkTask = Task.Run(async () =>
                {
                    Session session = await _connection._nativePubSubSessions.GetOrCreateSessionAsync()
                        .ConfigureAwait(false);
                    tmpSenderLink = new SenderLink(session, _id.ToString(), attach, onAttached);
                });

                // TODO configurable timeout
                TimeSpan waitSpan = TimeSpan.FromSeconds(5);

                _senderLink = await attachCompletedTcs.Task.WaitAsync(waitSpan)
                    .ConfigureAwait(false);

                await senderLinkTask.WaitAsync(waitSpan)
                    .ConfigureAwait(false);

                System.Diagnostics.Debug.Assert(tmpSenderLink != null);
                System.Diagnostics.Debug.Assert(Object.ReferenceEquals(_senderLink, tmpSenderLink));

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

            try
            {
                TaskCompletionSource<PublishOutcome> messagePublishedTcs =
                    new(TaskCreationOptions.RunContinuationsAsynchronously);
                Message nativeMessage = ((AmqpMessage)message).NativeMessage;

                void OutcomeCallback(ILink sender, Message inMessage, Outcome outcome, object state)
                {
                    System.Diagnostics.Debug.Assert(object.ReferenceEquals(this, state));
                    System.Diagnostics.Debug.Assert(object.ReferenceEquals(_senderLink, sender));
                    // Note: sometimes `message` is null 🤔
                    // System.Diagnostics.Debug.Assert(Object.ReferenceEquals(nativeMessage, message));

                    PublishOutcome publishOutcome;
                    switch (outcome)
                    {
                        case Rejected rejectedOutcome:
                            {
                                const OutcomeState publishState = OutcomeState.Rejected;
                                publishOutcome = new PublishOutcome(publishState, Utils.ConvertError(rejectedOutcome.Error));
                                break;
                            }
                        case Released:
                            {
                                const OutcomeState publishState = OutcomeState.Released;
                                publishOutcome = new PublishOutcome(publishState, null);
                                break;
                            }
                        case Accepted:
                            {
                                const OutcomeState publishState = OutcomeState.Accepted;
                                publishOutcome = new PublishOutcome(publishState, null);
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
                // TODO operation timeout
                // PublishOutcome publishOutcome = await messagePublishedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken)
                PublishOutcome publishOutcome = await messagePublishedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5))
                    .ConfigureAwait(false);

                return new PublishResult(message, publishOutcome);
            }
            catch (AmqpException ex)
            {
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

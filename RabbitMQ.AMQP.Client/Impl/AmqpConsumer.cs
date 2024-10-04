// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl
{
    public class AmqpConsumer : AbstractReconnectLifeCycle, IConsumer
    {
        private enum PauseStatus
        {
            UNPAUSED,
            PAUSING,
            PAUSED,
        }

        private readonly Guid _id = Guid.NewGuid();

        private ReceiverLink? _receiverLink;

        private PauseStatus _pauseStatus = PauseStatus.UNPAUSED;
        private readonly UnsettledMessageCounter _unsettledMessageCounter = new();
        private readonly ConsumerConfiguration _configuration;

        public AmqpConsumer(ConsumerConfiguration configuration)
        {
            _configuration = configuration;
            if (false == _configuration.Connection.Consumers.TryAdd(_id, this))
            {
                // TODO error?
            }
        }

        public override async Task OpenAsync()
        {
            try
            {
                TaskCompletionSource<ReceiverLink> attachCompletedTcs =
                    new(TaskCreationOptions.RunContinuationsAsynchronously);

                // this is an event to get the filters to the listener context
                // it _must_ be here because in case of reconnect the original filters could be not valid anymore
                // so the function must be called every time the consumer is opened normally or by reconnection
                // if ListenerContext is null the function will do nothing
                // ListenerContext will override only the filters the selected filters.
                _configuration.ListenerContext?.Invoke(
                    new IConsumerBuilder.ListenerContext(new ListenerStreamOptions(_configuration.Filters)));

                Attach attach = Utils.CreateAttach(_configuration.Address, DeliveryMode.AtLeastOnce, _id,
                    _configuration.Filters);

                void onAttached(ILink argLink, Attach argAttach)
                {
                    if (argLink is ReceiverLink link)
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

                ReceiverLink? tmpReceiverLink = null;
                Task receiverLinkTask = Task.Run(async () =>
                {
                    Session session = await _configuration.Connection._nativePubSubSessions.GetOrCreateSessionAsync()
                        .ConfigureAwait(false);
                    tmpReceiverLink = new ReceiverLink(session, _id.ToString(), attach, onAttached);
                });

                // TODO configurable timeout
                TimeSpan waitSpan = TimeSpan.FromSeconds(5);

                _receiverLink = await attachCompletedTcs.Task.WaitAsync(waitSpan)
                    .ConfigureAwait(false);

                await receiverLinkTask.WaitAsync(waitSpan)
                    .ConfigureAwait(false);

                System.Diagnostics.Debug.Assert(tmpReceiverLink != null);
                System.Diagnostics.Debug.Assert(object.ReferenceEquals(_receiverLink, tmpReceiverLink));

                if (_receiverLink is null)
                {
                    throw new ConsumerException($"{ToString()} Failed to create receiver link (null was returned)");
                }
                else if (_receiverLink.LinkState != LinkState.Attached)
                {
                    throw new ConsumerException(
                        $"{ToString()} Failed to create receiver link. Link state is not attached, error: " +
                        _receiverLink.Error?.ToString() ?? "Unknown error");
                }
                else
                {
                    _receiverLink.SetCredit(_configuration.InitialCredits);

                    // TODO save / cancel task
                    _ = Task.Run(ProcessMessages);

                    await base.OpenAsync()
                        .ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                throw new ConsumerException($"{ToString()} Failed to create receiver link, {e}");
            }
        }

        private async Task ProcessMessages()
        {
            try
            {
                if (_receiverLink is null)
                {
                    // TODO is this a serious error?
                    return;
                }

                while (_receiverLink is { LinkState: LinkState.Attached })
                {
                    // TODO the timeout waiting for messages should be configurable
                    TimeSpan timeout = TimeSpan.FromSeconds(60);
                    Message? nativeMessage = await _receiverLink.ReceiveAsync(timeout).ConfigureAwait(false);
                    if (nativeMessage is null)
                    {
                        // this is not a problem, it is just a timeout. 
                        // the timeout is set to 60 seconds. 
                        // For the moment I'd trace it at some point we can remove it
                        Trace.WriteLine(TraceLevel.Verbose,
                            $"{ToString()}: Timeout {timeout.Seconds} s.. waiting for message.");
                        continue;
                    }

                    _unsettledMessageCounter.Increment();

                    IContext context = new DeliveryContext(_receiverLink, nativeMessage, _unsettledMessageCounter);
                    var amqpMessage = new AmqpMessage(nativeMessage);

                    // TODO catch exceptions thrown by handlers,
                    // then call exception handler?
                    if (_configuration.Handler != null)
                    {
                        await _configuration.Handler(context, amqpMessage).ConfigureAwait(false);
                    }
                }
            }
            catch (Exception e)
            {
                if (State == State.Closing)
                {
                    return;
                }

                Trace.WriteLine(TraceLevel.Error, $"{ToString()} Failed to process messages, {e}");
                // TODO this is where a Listener should get a closed event
                // See the ConsumerShouldBeClosedWhenQueueIsDeleted test
            }

            Trace.WriteLine(TraceLevel.Verbose, $"{ToString()} is closed.");
        }

        public void Pause()
        {
            if (_receiverLink is null)
            {
                // TODO create "internal bug" exception type?
                throw new InvalidOperationException(
                    "_receiverLink is null, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
            }

            if ((int)PauseStatus.UNPAUSED == Interlocked.CompareExchange(
                    ref Unsafe.As<PauseStatus, int>(ref _pauseStatus),
                    (int)PauseStatus.PAUSING, (int)PauseStatus.UNPAUSED))
            {
                _receiverLink.SetCredit(credit: 0);

                if ((int)PauseStatus.PAUSING != Interlocked.CompareExchange(
                        ref Unsafe.As<PauseStatus, int>(ref _pauseStatus),
                        (int)PauseStatus.PAUSED, (int)PauseStatus.PAUSING))
                {
                    _pauseStatus = PauseStatus.UNPAUSED;
                    // TODO create "internal bug" exception type?
                    throw new InvalidOperationException(
                        "error transitioning from PAUSING -> PAUSED, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
                }
            }
            else
            {
                // TODO: log a warning that user tried to pause an already-paused consumer?
            }
        }

        public long UnsettledMessageCount => _unsettledMessageCounter.Get();

        public void Unpause()
        {
            if (_receiverLink is null)
            {
                // TODO create "internal bug" exception type?
                throw new InvalidOperationException(
                    "_receiverLink is null, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
            }

            if ((int)PauseStatus.PAUSED == Interlocked.CompareExchange(
                    ref Unsafe.As<PauseStatus, int>(ref _pauseStatus),
                    (int)PauseStatus.UNPAUSED, (int)PauseStatus.PAUSED))
            {
                _receiverLink.SetCredit(credit: _configuration.InitialCredits);
            }
            else
            {
                // TODO: log a warning that user tried to unpause a not-paused consumer?
            }
        }

        // TODO cancellation token
        public override async Task CloseAsync()
        {
            if (_receiverLink is null)
            {
                return;
            }

            OnNewStatus(State.Closing, null);

            try
            {
                // TODO global timeout for closing, other async actions?
                await _receiverLink.CloseAsync(TimeSpan.FromSeconds(5))
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Trace.WriteLine(TraceLevel.Warning, "Failed to close receiver link. The consumer will be closed anyway",
                    ex);
            }

            _receiverLink = null;
            OnNewStatus(State.Closed, null);
            _configuration.Connection.Consumers.TryRemove(_id, out _);
        }

        public override string ToString()
        {
            return $"Consumer{{Address='{_configuration.Address}', " +
                   $"id={_id}, " +
                   $"Connection='{_configuration.Connection}', " +
                   $"State='{State}'}}";
        }
    }
}

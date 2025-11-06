// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

namespace RabbitMQ.AMQP.Client.Impl
{
    static class ConsumerDefaults
    {
        public const int AttachTimeoutSeconds = 5;
        public const int MessageReceiveTimeoutSeconds = 60;
        public const int CloseTimeoutSeconds = 5;
        public const int AttachDelayMilliseconds = 10;
    }

    /// <summary>
    /// Implementation of <see cref="IConsumer"/>.
    /// </summary>
    public class AmqpConsumer : AbstractReconnectLifeCycle, IConsumer
    {
        private enum PauseStatus
        {
            UNPAUSED,
            PAUSING,
            PAUSED,
        }

        private readonly AmqpConnection _amqpConnection;
        private readonly Guid _id = Guid.NewGuid();

        private ReceiverLink? _receiverLink;
        private Attach? _attach;

        private PauseStatus _pauseStatus = PauseStatus.UNPAUSED;
        private readonly UnsettledMessageCounter _unsettledMessageCounter = new();
        private readonly ConsumerConfiguration _configuration;
        private readonly IMetricsReporter? _metricsReporter;

        internal AmqpConsumer(AmqpConnection amqpConnection, ConsumerConfiguration configuration,
            IMetricsReporter? metricsReporter)
        {
            _amqpConnection = amqpConnection;
            _configuration = configuration;
            _metricsReporter = metricsReporter;
            _amqpConnection.AddConsumer(_id, this);
        }

        public override async Task OpenAsync()
        {
            try
            {
                TaskCompletionSource<(ReceiverLink, Attach)> attachCompletedTcs =
                    Utils.CreateTaskCompletionSource<(ReceiverLink, Attach)>();

                // this is an event to get the filters to the listener context
                // it _must_ be here because in case of reconnect the original filters could be not valid anymore
                // so the function must be called every time the consumer is opened normally or by reconnection
                // if ListenerContext is null the function will do nothing
                // ListenerContext will override only the filters the selected filters.
                if (_configuration.ListenerContext is not null)
                {
                    var listenerStreamOptions = new ListenerStreamOptions(_configuration.Filters);
                    var listenerContext = new IConsumerBuilder.ListenerContext(listenerStreamOptions);
                    _configuration.ListenerContext(listenerContext);
                }

                Attach attach;

                if (_configuration.DirectReplyTo)
                {
                    attach = Utils.CreateDirectReplyToAttach(_id, _configuration.Filters);
                }
                else
                {
                    string address = AddressBuilderHelper.AddressBuilder().Queue(_configuration.Queue).Address();
                    attach = Utils.CreateAttach(address, DeliveryMode.AtLeastOnce, _id,
                        _configuration.Filters);
                }

                void OnAttached(ILink argLink, Attach argAttach)
                {
                    if (argLink is ReceiverLink link)
                    {
                        attachCompletedTcs.SetResult((link, argAttach));
                    }
                    else
                    {
                        // TODO create "internal bug" exception type?
                        var ex = new InvalidOperationException(
                            "invalid link in OnAttached, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
                        attachCompletedTcs.SetException(ex);
                    }
                }

                Session session = await _amqpConnection._nativePubSubSessions.GetOrCreateSessionAsync()
                    .ConfigureAwait(false);

                var tmpReceiverLink = new ReceiverLink(session, _id.ToString(), attach, OnAttached);

                // TODO configurable timeout
                var waitSpan = TimeSpan.FromSeconds(ConsumerDefaults.AttachTimeoutSeconds);

                // TODO
                // Even 10ms is enough to allow the links to establish,
                // which tells me it allows the .NET runtime to process
                await Task.Delay(ConsumerDefaults.AttachDelayMilliseconds).ConfigureAwait(false);

                (_receiverLink, _attach) = await attachCompletedTcs.Task.WaitAsync(waitSpan)
                    .ConfigureAwait(false);
                ValidateReceiverLink();

                _receiverLink.SetCredit(_configuration.InitialCredits);

                // TODO save / cancel task
                _ = Task.Run(ProcessMessages);

                // TODO cancellation token
                await base.OpenAsync()
                    .ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new ConsumerException($"{ToString()} Failed to create receiver link, {e}");
            }
        }

        private void ValidateReceiverLink()
        {
            if (_receiverLink is null)
            {
                throw new ConsumerException($"{ToString()} Receiver link creation failed (null was returned)");
            }

            if (_receiverLink.LinkState != LinkState.Attached)
            {
                string errorMessage = _receiverLink.Error?.ToString() ?? "Unknown error";
                throw new ConsumerException($"{ToString()} Receiver link not attached. Error: {errorMessage}");
            }
        }

        private async Task ProcessMessages()
        {
            try
            {
                Stopwatch? stopwatch = null;
                if (_metricsReporter is not null)
                {
                    stopwatch = new();
                }

                while (_receiverLink is { LinkState: LinkState.Attached })
                {
                    stopwatch?.Restart();

                    // TODO the timeout waiting for messages should be configurable
                    TimeSpan timeout = TimeSpan.FromSeconds(ConsumerDefaults.MessageReceiveTimeoutSeconds);
                    Message? nativeMessage = await _receiverLink.ReceiveAsync(timeout)
                        .ConfigureAwait(false);

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

                    IContext context = new DeliveryContext(_receiverLink, nativeMessage,
                        _unsettledMessageCounter, _metricsReporter);
                    var amqpMessage = new AmqpMessage(nativeMessage);

                    // TODO catch exceptions thrown by handlers,
                    // then call exception handler?
                    if (_configuration.Handler is not null)
                    {
                        await _configuration.Handler(context, amqpMessage)
                            .ConfigureAwait(false);
                    }

                    if (_metricsReporter is not null && stopwatch is not null)
                    {
                        stopwatch.Stop();
                        _metricsReporter.Consumed(stopwatch.Elapsed);
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

        /// <summary>
        /// Pause the consumer to stop receiving messages.
        /// </summary>
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

        /// <summary>
        /// Get the number of unsettled messages.
        /// </summary>
        public long UnsettledMessageCount => _unsettledMessageCounter.Get();

        public string Queue
        {
            get
            {
                string? sourceAddress = _attach?.Source is not Source source ? null : source.Address;
                return sourceAddress is null ? "" : AddressBuilderHelper.AddressBuilder().DecodeQueuePathSegment(sourceAddress);
            }
        }

        /// <summary>
        /// Request to receive messages again.
        /// </summary>
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
                await _receiverLink.CloseAsync(TimeSpan.FromSeconds(ConsumerDefaults.CloseTimeoutSeconds))
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Trace.WriteLine(TraceLevel.Warning, "Failed to close receiver link. The consumer will be closed anyway",
                    ex);
            }

            _receiverLink = null;
            OnNewStatus(State.Closed, null);
            _amqpConnection.RemoveConsumer(_id);
        }

        public override string ToString()
        {
            return $"Consumer{{Address='{Queue}', " +
                   $"id={_id}, " +
                   $"Connection='{_amqpConnection}', " +
                   $"State='{State}'}}";
        }
    }
}

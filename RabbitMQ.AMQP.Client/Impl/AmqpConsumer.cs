// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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
    /// Default timeouts and delays used by the consumer implementation.
    /// </summary>
    static class ConsumerDefaults
    {
        public const int AttachTimeoutSeconds = 5;
        public const int MessageReceiveTimeoutSeconds = 60;
        public const int CloseTimeoutSeconds = 5;
        public const int AttachDelayMilliseconds = 10;
    }

    /// <summary>
    /// Implementation of <see cref="IConsumer"/>.
    /// Receives messages from a queue and dispatches them to the configured
    /// <see cref="ConsumerConfiguration.Handler"/> (see <see cref="IConsumerBuilder.MessageHandler"/>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Handler safety:</b> The code passed as <c>configuration.Handler</c> (via
    /// <see cref="IConsumerBuilder.MessageHandler"/>) runs on the consumer's message-processing loop.
    /// To keep the consumer safe and predictable:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>Handle all exceptions inside the handler; unhandled exceptions can break the processing loop and stop message delivery.</description></item>
    ///   <item><description>Always settle each message via <see cref="IContext"/> (Accept, Discard, or Requeue) when using explicit settlement; failing to do so leaves messages unsettled and can exhaust credits.</description></item>
    /// </list>
    /// </remarks>
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

        /// <summary>
        /// Creates a consumer for the given connection with the specified configuration.
        /// </summary>
        /// <param name="amqpConnection">The connection to use.</param>
        /// <param name="configuration">Consumer configuration; <see cref="ConsumerConfiguration.Handler"/> must be set and its code must be safe (see class remarks).</param>
        /// <param name="metricsReporter">Optional metrics reporter.</param>
        internal AmqpConsumer(AmqpConnection amqpConnection, ConsumerConfiguration configuration,
            IMetricsReporter? metricsReporter)
        {
            _amqpConnection = amqpConnection;
            _configuration = configuration;
            _metricsReporter = metricsReporter;
            _amqpConnection.AddConsumer(_id, this);
        }

        /// <summary>
        /// Opens the consumer: attaches the receiver link, applies filters/listener context, and starts the message-processing loop.
        /// </summary>
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

                if (_configuration.SettleStrategy == ConsumerSettleStrategy.DirectReplyTo)
                {
                    attach = Utils.CreateDirectReplyToAttach(_id, _configuration.Filters);
                }
                else
                {
                    string address = AddressBuilderHelper.AddressBuilder().Queue(_configuration.Queue).Address();
                    attach = Utils.CreateAttach(address, DeliveryMode.AtLeastOnce, _id,
                        _configuration.Filters, _configuration.SettleStrategy == ConsumerSettleStrategy.PreSettled);
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

                if (_configuration.SingleActiveConsumerStateChangedHandler is not null &&
                    _configuration.IsQuorumQueueFromSpecification)
                {
                    _receiverLink.OnLinkStateProperties = OnFlowLinkStateProperties;
                }

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

        private void OnFlowLinkStateProperties(ILink _, Map? properties)
        {
            if (properties is null || properties.Count == 0)
            {
                return;
            }

            if (false == TryGetRabbitMqActive(properties, out bool isActive))
            {
                return;
            }

            SingleActiveConsumerStateHandler? handler = _configuration.SingleActiveConsumerStateChangedHandler;
            if (handler is null)
            {
                return;
            }

            try
            {
                handler(this, isActive);
            }
            catch (Exception ex)
            {
                Trace.WriteLine(TraceLevel.Error,
                    $"{ToString()} SingleActiveConsumerStateChanged handler threw an exception", ex);
            }
        }

        private static bool TryGetRabbitMqActive(Map properties, out bool isActive)
        {
            foreach (KeyValuePair<object, object?> kv in properties)
            {
                string keyString = kv.Key is Symbol sym ? sym.ToString() : kv.Key?.ToString() ?? string.Empty;
                if (keyString != Consts.RabbitMqActiveProperty)
                {
                    continue;
                }

                object? value = kv.Value;
                switch (value)
                {
                    case bool b:
                        isActive = b;
                        return true;
                    case byte by:
                        isActive = by != 0;
                        return true;
                    case int i:
                        isActive = i != 0;
                        return true;
                    case long l:
                        isActive = l != 0;
                        return true;
                    case uint u:
                        isActive = u != 0;
                        return true;
                    case ulong ul:
                        isActive = ul != 0;
                        return true;
                }
            }

            isActive = false;
            return false;
        }

        /// <summary>
        /// Ensures the receiver link was created and attached successfully; throws otherwise.
        /// </summary>
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

        /// <summary>
        /// Runs the message loop: receives messages and invokes <see cref="ConsumerConfiguration.Handler"/> for each delivery.
        /// Handler code must be safe (handle exceptions, settle messages, avoid blocking); see class remarks.
        /// </summary>
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

                    IContext context = _configuration.SettleStrategy switch
                    {
                        ConsumerSettleStrategy.PreSettled => new PreSettledDeliveryContext(),
                        _ => new DeliveryContext(_receiverLink, nativeMessage, _unsettledMessageCounter,
                            _metricsReporter)
                    };

                    if (_configuration.SettleStrategy != ConsumerSettleStrategy.PreSettled)
                    {
                        _unsettledMessageCounter.Increment();
                    }

                    var amqpMessage = new AmqpMessage(nativeMessage);

                    // Invoke the user handler. Handler code must be safe: catch exceptions,
                    // settle the message (Accept/Discard/Requeue), and avoid blocking so the loop can continue.
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

        /// <summary>
        /// Gets the queue name this consumer is attached to (from the link source address).
        /// </summary>
        public string Queue
        {
            get
            {
                string? sourceAddress = _attach?.Source is not Source source ? null : source.Address;
                return sourceAddress is null
                    ? ""
                    : AddressBuilderHelper.AddressBuilder().DecodeQueuePathSegment(sourceAddress);
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

        /// <summary>
        /// Closes the consumer and detaches the receiver link.
        /// </summary>
        public override async Task CloseAsync()
        {
            if (_receiverLink is null)
            {
                return;
            }

            OnNewStatus(State.Closing, null);

            try
            {
                _receiverLink.OnLinkStateProperties = null;
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

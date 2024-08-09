// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Runtime.CompilerServices;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpConsumer : AbstractReconnectLifeCycle, IConsumer
{
    private enum PauseStatus : byte
    {
        UNPAUSED = 0,
        PAUSING = 1,
        PAUSED = 2
    }

    private readonly AmqpConnection _connection;
    private readonly string _address;
    private readonly MessageHandler _messageHandler;
    private readonly int _initialCredits;
    private readonly Map _filters;
    private ReceiverLink? _receiverLink;
    private PauseStatus _pauseStatus = PauseStatus.UNPAUSED;
    private readonly UnsettledMessageCounter _unsettledMessageCounter = new();

    public AmqpConsumer(AmqpConnection connection, string address,
        MessageHandler messageHandler, int initialCredits, Map filters)
    {
        _connection = connection;
        _address = address;
        _messageHandler = messageHandler;
        _initialCredits = initialCredits;
        _filters = filters;
        _connection.Consumers.TryAdd(Id, this);
    }

    public override async Task OpenAsync()
    {
        try
        {
            TaskCompletionSource attachCompletedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

            Attach attach = Utils.CreateAttach(_address, DeliveryMode.AtLeastOnce, Id, _filters);

            void onAttached(ILink argLink, Attach argAttach)
            {
                attachCompletedTcs.SetResult();
            }

            Task receiverLinkTask = Task.Run(() =>
            {
                _receiverLink = new ReceiverLink(_connection._nativePubSubSessions.GetOrCreateSession(), Id, attach, onAttached);
            });

            // TODO configurable timeout
            TimeSpan waitSpan = TimeSpan.FromSeconds(5);

            await attachCompletedTcs.Task.WaitAsync(waitSpan)
                .ConfigureAwait(false);

            await receiverLinkTask.WaitAsync(waitSpan)
                .ConfigureAwait(false);

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
                // TODO: Check the performance during the download messages
                // The publisher is faster than the consumer
                _receiverLink.Start(_initialCredits, OnReceiverLinkMessage);

                await base.OpenAsync()
                    .ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            throw new ConsumerException($"{ToString()} Failed to create receiver link, {e}");
        }
    }

    private void OnReceiverLinkMessage(IReceiverLink link, Message message)
    {
        _unsettledMessageCounter.Increment();
        IContext context = new DeliveryContext(link, message, _unsettledMessageCounter);
        _messageHandler(context, new AmqpMessage(message));
    }

    private string Id { get; } = Guid.NewGuid().ToString();

    public void Pause()
    {
        if (_receiverLink is null)
        {
            // TODO create "internal bug" exception type?
            throw new InvalidOperationException("_receiverLink is null, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
        }

        if ((int)PauseStatus.UNPAUSED == Interlocked.CompareExchange(ref Unsafe.As<PauseStatus, int>(ref _pauseStatus),
            (int)PauseStatus.PAUSING, (int)PauseStatus.UNPAUSED))
        {
            _receiverLink.SetCredit(credit: 0, autoRestore: false);

            if ((int)PauseStatus.PAUSING != Interlocked.CompareExchange(ref Unsafe.As<PauseStatus, int>(ref _pauseStatus),
                (int)PauseStatus.PAUSED, (int)PauseStatus.PAUSING))
            {
                _pauseStatus = PauseStatus.UNPAUSED;
                // TODO create "internal bug" exception type?
                throw new InvalidOperationException("error transitioning from PAUSING -> PAUSED, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
            }
        }
        else
        {
            // TODO: log a warning that user tried to pause an already-paused consumer?
        }
    }

    public long UnsettledMessageCount
    {
        get
        {
            return _unsettledMessageCounter.Get();
        }
    }

    public void Unpause()
    {
        if (_receiverLink is null)
        {
            // TODO create "internal bug" exception type?
            throw new InvalidOperationException("_receiverLink is null, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
        }

        if ((int)PauseStatus.PAUSED == Interlocked.CompareExchange(ref Unsafe.As<PauseStatus, int>(ref _pauseStatus),
            (int)PauseStatus.UNPAUSED, (int)PauseStatus.PAUSED))
        {
            _receiverLink.SetCredit(credit: _initialCredits);
        }
        else
        {
            // TODO: log a warning that user tried to unpause a not-paused consumer?
        }
    }

    public override async Task CloseAsync()
    {
        if (_receiverLink == null)
        {
            return;
        }

        OnNewStatus(State.Closing, null);

        // TODO timeout
        await _receiverLink.CloseAsync()
            .ConfigureAwait(false);

        _receiverLink = null;

        OnNewStatus(State.Closed, null);

        _connection.Consumers.TryRemove(Id, out _);
    }

    public override string ToString()
    {
        return $"Consumer{{Address='{_address}', id={Id} ConnectionName='{_connection}', State='{State}'}}";
    }
}

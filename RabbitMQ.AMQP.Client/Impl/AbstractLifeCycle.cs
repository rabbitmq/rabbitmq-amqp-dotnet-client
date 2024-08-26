// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpNotOpenException(string message) : Exception(message);

public abstract class AbstractLifeCycle : ILifeCycle
{
    protected bool _disposed;

    public virtual Task OpenAsync()
    {
        OnNewStatus(State.Open, null);
        return Task.CompletedTask;
    }

    public abstract Task CloseAsync();

    public State State { get; internal set; } = State.Closed;

    public event LifeCycleCallBack? ChangeState;

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    protected void ThrowIfClosed()
    {
        switch (State)
        {
            case State.Closed:
                throw new AmqpNotOpenException("Resource is closed");
            case State.Closing:
                throw new AmqpNotOpenException("Resource is closing");
            case State.Reconnecting:
                throw new AmqpNotOpenException("Resource is Reconnecting");
            case State.Open:
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    protected void OnNewStatus(State newState, Error? error)
    {
        if (State == newState)
        {
            return;
        }

        State oldStatus = State;
        State = newState;
        ChangeState?.Invoke(this, oldStatus, newState, error);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (false == _disposed)
        {
            if (disposing)
            {
                // TODO: dispose managed state (managed objects)
            }

            // TODO: free unmanaged resources (unmanaged objects) and override finalizer
            // TODO: set large fields to null
            _disposed = true;
        }
    }

    // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
    // ~AbstractLifeCycle()
    // {
    //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
    //     Dispose(disposing: false);
    // }
}

public abstract class AbstractReconnectLifeCycle : AbstractLifeCycle
{
    private readonly BackOffDelayPolicy _backOffDelayPolicy = BackOffDelayPolicy.Create(2);

    internal void ChangeStatus(State newState, Error? error)
    {
        OnNewStatus(newState, error);
    }

    internal async Task ReconnectAsync()
    {
        try
        {
            int randomWait = Random.Shared.Next(300, 900);
            Trace.WriteLine(TraceLevel.Information, $"{ToString()} is reconnecting in {randomWait} ms, " +
                                                    $"attempt: {_backOffDelayPolicy.CurrentAttempt}");
            await Task.Delay(randomWait)
                .ConfigureAwait(false);

            await OpenAsync()
                .ConfigureAwait(false);

            Trace.WriteLine(TraceLevel.Information,
                $"{ToString()} is reconnected, attempt: {_backOffDelayPolicy.CurrentAttempt}");

            _backOffDelayPolicy.Reset();
        }
        catch (Exception e)
        {
            // Here we give another chance to reconnect
            // that's an edge case, where the link is not ready for some reason
            // the backoff policy will be used to delay the reconnection and give just a few attempts
            Trace.WriteLine(TraceLevel.Error, $"{ToString()} Failed to reconnect, {e.Message}");

            int delay = _backOffDelayPolicy.Delay();
            await Task.Delay(delay)
                .ConfigureAwait(false);

            if (_backOffDelayPolicy.IsActive())
            {
                await ReconnectAsync()
                    .ConfigureAwait(false);
            }
        }
    }
}

using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpNotOpenException(string message) : Exception(message);

public abstract class AbstractLifeCycle : ILifeCycle
{
    protected virtual Task OpenAsync()
    {
        OnNewStatus(State.Open, null);
        return Task.CompletedTask;
    }

    public abstract Task CloseAsync();

    public State State { get; internal set; } = State.Closed;

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

    // wait until the close operation is completed
    protected readonly TaskCompletionSource<bool> ConnectionCloseTaskCompletionSource =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

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

    public event LifeCycleCallBack? ChangeState;
}

public abstract class AbstractReconnectLifeCycle : AbstractLifeCycle
{
    private readonly BackOffDelayPolicy _backOffDelayPolicy = BackOffDelayPolicy.Create(2);

    internal void ChangeStatus(State newState, Error? error)
    {
        OnNewStatus(newState, error);
    }

    internal async Task Reconnect()
    {
        try
        {
            int randomWait = Random.Shared.Next(300, 900);
            Trace.WriteLine(TraceLevel.Information, $"{ToString()} is reconnecting in {randomWait} ms, " +
                                                    $"attempt: {_backOffDelayPolicy.CurrentAttempt}");
            await Task.Delay(randomWait).ConfigureAwait(false);
            await OpenAsync().ConfigureAwait(false);
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
            await Task.Delay(delay).ConfigureAwait(false);
            if (_backOffDelayPolicy.IsActive())
            {
                await Reconnect().ConfigureAwait(false);
            }
        }
    }
}

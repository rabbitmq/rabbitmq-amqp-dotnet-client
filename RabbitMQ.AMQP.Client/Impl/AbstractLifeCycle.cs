namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpClosedException(string message) : Exception(message);

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
        if (State == State.Closed)
        {
            throw new AmqpClosedException(GetType().Name);
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

        var oldStatus = State;
        State = newState;
        ChangeState?.Invoke(this, oldStatus, newState, error);
    }

    public event LifeCycleCallBack? ChangeState;
}

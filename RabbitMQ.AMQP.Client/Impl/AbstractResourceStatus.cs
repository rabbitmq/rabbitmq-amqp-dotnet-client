namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpClosedException(string message) : Exception(message);

public abstract class AbstractResourceStatus : IResourceStatus
{
    public State State { get; internal set; } = State.Closed;
    protected void ThrowIfClosed()
    {
        if (State == State.Closed)
        {
            throw new AmqpClosedException(GetType().Name);
        }
    }


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

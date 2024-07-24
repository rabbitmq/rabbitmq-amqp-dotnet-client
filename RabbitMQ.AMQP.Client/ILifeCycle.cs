namespace RabbitMQ.AMQP.Client;

public enum State
{
    // Opening,
    Open,
    Reconnecting,
    Closing,
    Closed,
}

public class Error(string? errorCode, string? description)
{
    public string? Description { get; } = description;
    public string? ErrorCode { get; } = errorCode;

    public override string ToString()
    {
        return $"Code: {ErrorCode} - Description: {Description}";
    }
}

public delegate void LifeCycleCallBack(object sender, State previousState, State currentState, Error? failureCause);

public interface ILifeCycle
{
    Task CloseAsync();

    public State State { get; }

    public event LifeCycleCallBack ChangeState;
}

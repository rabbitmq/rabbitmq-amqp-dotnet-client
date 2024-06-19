namespace RabbitMQ.AMQP.Client;

public enum State
{
    // Opening,
    Open,
    Reconnecting,
    Closing,
    Closed,
}

public class Error
{
    public string? Description { get; internal set; }
    public string? ErrorCode { get; internal set; }
}

public interface IClosable
{
    public State State { get;  }

    Task CloseAsync();

    public delegate void LifeCycleCallBack(object sender, State previousState, State currentState, Error? failureCause);

    event LifeCycleCallBack ChangeState;
}
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
    
    public override string ToString()
    {
        return $"Code: {ErrorCode} - Description: {Description}";
    }
}

public interface IClosable // TODO: Create an abstract class with the event and the State property
{
    public State State { get; }

    Task CloseAsync();

    public delegate void LifeCycleCallBack(object sender, State previousState, State currentState, Error? failureCause);

    event LifeCycleCallBack ChangeState;
}
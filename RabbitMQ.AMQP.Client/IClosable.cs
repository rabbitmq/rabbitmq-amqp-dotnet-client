namespace RabbitMQ.AMQP.Client;

public enum Status
{
    Closed,
    Reconneting,
    Open,
}

public class Error
{
    public string? Description { get; internal set; }
    public string? ErrorCode { get; internal set; }
}

public interface IClosable
{
    public Status Status { get;  }

    Task CloseAsync();

    public delegate void ChangeStatusCallBack(object sender, Status from, Status to, Error? error);

    event ChangeStatusCallBack ChangeStatus;
     
    
}
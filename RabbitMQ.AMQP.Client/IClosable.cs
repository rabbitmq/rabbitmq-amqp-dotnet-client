namespace RabbitMQ.AMQP.Client;

public enum Status
{
    Closed,
    Open,
}

public interface IClosable
{
    public Status Status { get; }

    Task CloseAsync();

    public delegate void ClosedEventHandler(object sender, bool unexpected);

    event ClosedEventHandler Closed;
}
namespace RabbitMQ.AMQP.Client;

public enum Status
{
    Closed,
    Open,
}

public interface IResource
{
    public Status Status { get; }

    Task CloseAsync();

    public delegate void ClosedEventHandler(object sender, bool unexpected);

    event ClosedEventHandler Closed;
}
namespace RabbitMQ.AMQP.Client;

public class ConnectionException(string? message) : Exception(message);

public interface IConnection
{
    IManagement Management();
    Task ConnectAsync();
}

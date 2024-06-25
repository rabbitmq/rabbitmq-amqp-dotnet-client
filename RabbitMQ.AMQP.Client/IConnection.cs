namespace RabbitMQ.AMQP.Client;

public class ConnectionException(string? message, Exception? innerException) : Exception(message, innerException);

public interface IConnection 
{
    IManagement Management();
    Task ConnectAsync();
}
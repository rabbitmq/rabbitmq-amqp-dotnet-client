namespace RabbitMQ.AMQP.Client;

public interface IConnection
{
    IManagement Management();
    Task ConnectAsync(IAddress amqpAddress);
    Task CloseAsync();
}
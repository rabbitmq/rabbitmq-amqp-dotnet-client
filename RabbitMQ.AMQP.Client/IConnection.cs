namespace RabbitMQ.AMQP.Client;

public interface IConnection
{
    IManagement Management();
    Task ConnectAsync(AmqpAddress amqpAddress);
    Task CloseAsync();
}
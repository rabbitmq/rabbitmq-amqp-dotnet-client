namespace RabbitMQ.AMQP.Client;

public class InvalidAddressException(string message) : Exception(message);

public interface IAddressBuilder<out T>
{
    T Exchange(IExchangeSpecification exchangeSpec);
    T Exchange(string exchangeName);

    T Queue(IQueueSpecification queueSpec);
    T Queue(string queueName);

    T Key(string key);
}

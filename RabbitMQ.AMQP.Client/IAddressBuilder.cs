namespace RabbitMQ.AMQP.Client;

public class InvalidAddressException(string message) : Exception(message);

public interface IAddressBuilder<out T>
{

    T Exchange(string exchange);

    T Queue(string queue);

    T Key(string key);
}
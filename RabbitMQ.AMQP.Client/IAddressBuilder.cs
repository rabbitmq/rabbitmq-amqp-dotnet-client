namespace RabbitMQ.AMQP.Client;

public interface IAddressBuilder<out T>
{
    
    T Exchange(string exchange);

    T Key(string key);

    T Queue(string queue);
}
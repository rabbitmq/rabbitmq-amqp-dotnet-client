namespace RabbitMQ.AMQP.Client;

public interface ITopologyListener
{
    void QueueDeclared(IQueueSpecification specification);

    void QueueDeleted(string name);

    void ExchangeDeclared(IExchangeSpecification specification);

    void ExchangeDeleted(string name);

    void Clear();

    int QueueCount();

    int ExchangeCount();
}

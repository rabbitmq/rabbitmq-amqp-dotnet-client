namespace RabbitMQ.AMQP.Client;

public interface ITopologyListener
{
    void QueueDeclared(IQueueSpecification specification);

    void QueueDeleted(string name);

    void Clear();

    int QueueCount();
}
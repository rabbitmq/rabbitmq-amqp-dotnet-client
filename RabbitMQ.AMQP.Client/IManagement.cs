namespace RabbitMQ.AMQP.Client;

public interface IManagement
{
    IQueueSpecification Queue();
    IQueueSpecification Queue(string name);
}

public interface IQueueSpecification
{
    IQueueSpecification Name(string name);
    IQueueSpecification Exclusive(bool exclusive);
    IQueueSpecification AutoDelete(bool autoDelete);
}
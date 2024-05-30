namespace RabbitMQ.AMQP.Client;

public enum ManagementStatus
{
    Closed,
    Initializing,
    Open,
}

public interface IManagement
{
    ManagementStatus Status { get;}
    IQueueSpecification Queue();
    IQueueSpecification Queue(string name);
}

public interface IQueueSpecification
{
    IQueueSpecification Name(string name);
    IQueueSpecification Exclusive(bool exclusive);
    IQueueSpecification AutoDelete(bool autoDelete);
    
    IQueueSpecification Durable(bool durable);
    
    Task<IQueueInfo> Declare();
}
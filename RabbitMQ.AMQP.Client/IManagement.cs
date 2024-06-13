namespace RabbitMQ.AMQP.Client;

public class ModelException(string message) : Exception(message);

public class PreconditionFailException(string message) : Exception(message);

public interface IManagement : IClosable
{
    IQueueSpecification Queue();
    IQueueSpecification Queue(string name);

    IQueueDeletion QueueDeletion();
    
    ITopologyListener TopologyListener();
}


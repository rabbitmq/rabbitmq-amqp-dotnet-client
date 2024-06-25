namespace RabbitMQ.AMQP.Client;

public class PublisherException : Exception
{
    public PublisherException(string message, Exception innerException) : base(message)
    {
        
    }
}

public interface IPublisher : IClosable
{
    Task Publish(IMessage message); // TODO: Add CancellationToken and callBack
}
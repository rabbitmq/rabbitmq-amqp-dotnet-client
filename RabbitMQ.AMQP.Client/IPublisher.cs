namespace RabbitMQ.AMQP.Client;

public interface IPublisher: IClosable
{
    Task Publish(IMessage message); // TODO: Add CancellationToken and callBack

}
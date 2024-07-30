namespace RabbitMQ.AMQP.Client;

public class ConsumerException(string message) : Exception(message);
public delegate Task MessageHandler(IContext context, IMessage message);

public interface IConsumer : ILifeCycle
{
    void Pause();
    void Unpause();
    long UnsettledMessageCount { get; }
}

public interface IContext
{
    Task AcceptAsync();
    Task DiscardAsync();
    Task RequeueAsync();
}

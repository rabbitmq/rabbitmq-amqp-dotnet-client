using Amqp.Listener;

namespace RabbitMQ.AMQP.Client;

public class ConsumerException(string message) : Exception(message);
public delegate Task MessageHandler(IContext context, IMessage message);

public interface IConsumer : ILifeCycle
{
    void Pause();

    long UnsettledMessageCount();

    void Unpause();
}

public interface IMessageHandler
{
    void Handle(Context context, IMessage message);
}

public interface IContext
{
    Task Accept();

    Task Discard();

    Task Requeue();
}

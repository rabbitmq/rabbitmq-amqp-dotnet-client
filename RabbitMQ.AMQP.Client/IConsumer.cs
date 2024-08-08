using Amqp.Listener;

namespace RabbitMQ.AMQP.Client;

public class ConsumerException(string message) : Exception(message);
public delegate void MessageHandler(IContext context, IMessage message);

public interface IConsumer : ILifeCycle
{
    void Pause();

    void Unpause();

    long UnsettledMessageCount { get; }
}

public interface IMessageHandler
{
    void Handle(Context context, IMessage message);
}

public interface IContext
{
    void Accept();

    void Discard();

    void Requeue();
}

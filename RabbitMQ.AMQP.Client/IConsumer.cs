using Amqp.Listener;

namespace RabbitMQ.AMQP.Client;

public delegate void MessageHandler(Context context, IMessage message);


public interface IConsumer : IClosable
{
   
}


public interface IMessageHandler {

    void Handle(Context context, IMessage message);
}

public interface IContext {

    void Accept();

    void Discard();

    void Requeue();
}

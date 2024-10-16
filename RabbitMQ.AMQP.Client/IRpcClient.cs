using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{

    public interface IRpcClientAddressBuilder : IAddressBuilder<IRpcClientAddressBuilder>
    {
        IRpcClientBuilder RpcClient();
    }

    public interface IRpcClientBuilder
    {
        IRpcClientAddressBuilder RequestAddress();
        IRpcClientBuilder ReplyToQueue(string replyToQueueName);

        IRpcClientBuilder ReplyToQueue(IQueueSpecification replyToQueue);
        IRpcClientBuilder CorrelationIdExtractor(Func<IMessage, object>? correlationIdExtractor);

        IRpcClientBuilder RequestPostProcessor(Func<IMessage, object, IMessage>? requestPostProcessor);

        IRpcClientBuilder CorrelationIdSupplier(Func<object>? correlationIdSupplier);
        IRpcClientBuilder Timeout(TimeSpan timeout);
        Task<IRpcClient> BuildAsync();
    }

    public interface IRpcClient : ILifeCycle
    {
        Task<IMessage> PublishAsync(IMessage message, CancellationToken cancellationToken = default);
    }
}

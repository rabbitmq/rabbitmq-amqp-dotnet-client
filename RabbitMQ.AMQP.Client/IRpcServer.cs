using System;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    public delegate Task<IMessage> RpcHandler(IRpcServer.IContext context, IMessage request);

    public interface IRpcServerBuilder
    {
        IRpcServerBuilder RequestQueue(string requestQueue);
        IRpcServerBuilder RequestQueue(IQueueSpecification requestQueue);
        IRpcServerBuilder CorrelationIdExtractor(Func<IMessage, object> correlationIdExtractor);
        IRpcServerBuilder Handler(RpcHandler handler);

        Task<IRpcServer> BuildAsync();
    }

    public interface IRpcServer : ILifeCycle
    {

        public interface IContext
        {
            IMessage Message(object body);
        }
    }
}

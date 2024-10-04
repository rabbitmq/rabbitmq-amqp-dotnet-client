using System;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    public interface IRpcServerBuilder
    {
        IRpcServerBuilder RequestQueue(string requestQueue);
        IRpcServerBuilder RequestQueue(IQueueSpecification requestQueue);
        IRpcServerBuilder CorrelationIdExtractor(Func<IMessage, object> correlationIdExtractor);
        IRpcServerBuilder Handler(IRpcServer.IHandler handler);
    }

    public interface IRpcServer : ILifeCycle
    {
        public IHandler? Handler { get; }

        public interface IHandler
        {
            IMessage Handle(IContext ctx, IMessage request);
        }

        public IContext Context { get; }

        public interface IContext
        {
            IMessage Message(object body);
        }
    }
}

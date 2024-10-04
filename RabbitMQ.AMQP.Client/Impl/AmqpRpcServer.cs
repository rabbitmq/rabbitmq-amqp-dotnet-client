using System;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client.Impl
{
    public class AmqpRpcServer : AbstractLifeCycle, IRpcServer
    {
        private AmqpConnection _connection;

        public AmqpRpcServer(AmqpConnection connection)
        {
            _connection = connection;
        }

        private class RpcServerHandler : IRpcServer.IHandler
        {
            public IMessage Handle(IRpcServer.IContext ctx, IMessage request) =>
                throw new System.NotImplementedException();
        }

        private class RpcServerContext : IRpcServer.IContext
        {
            public IMessage Message(object body) => throw new System.NotImplementedException();
        }



        public override Task CloseAsync() => throw new System.NotImplementedException();

        public IRpcServer.IHandler Handler { get; } = new RpcServerHandler();
        public IRpcServer.IContext Context { get; } = new RpcServerContext();
    }
}

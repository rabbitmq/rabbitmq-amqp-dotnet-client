using System;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client.Impl
{
    public class RpcConfiguration
    {
        public AmqpConnection Connection { get; set; } = null!;
        public RpcHandler? Handler { get; set; }
        public string RequestQueue { get; set; } = "";
    }

    public class AmqpRpcServerBuilder : IRpcServerBuilder
    {
        RpcConfiguration _configuration = new RpcConfiguration();

        public AmqpRpcServerBuilder(AmqpConnection connection)
        {
            _configuration.Connection = connection;
        }

        public IRpcServerBuilder RequestQueue(string requestQueue)
        {
            _configuration.RequestQueue = requestQueue;
            return this;
        }

        public IRpcServerBuilder RequestQueue(IQueueSpecification requestQueue)
        {
            _configuration.RequestQueue = requestQueue.QueueName;
            return this;
        }

        public IRpcServerBuilder CorrelationIdExtractor(Func<IMessage, object> correlationIdExtractor) =>
            throw new NotImplementedException();

        public IRpcServerBuilder Handler(RpcHandler handler)
        {
            _configuration.Handler = handler;
            return this;
        }

        public async Task<IRpcServer> BuildAsync()
        {
            AmqpRpcServer amqpRpcServer = new(_configuration);
            await amqpRpcServer.OpenAsync().ConfigureAwait(false);
            return amqpRpcServer;
        }
    }

    public class AmqpRpcServer : AbstractLifeCycle, IRpcServer
    {
        private readonly RpcConfiguration _configuration;
        private IPublisher? _publisher = null;
        private IConsumer? _consumer = null;

        public AmqpRpcServer(RpcConfiguration builder)
        {
            _configuration = builder;
        }

        public override async Task OpenAsync()
        {
            _publisher = await _configuration.Connection.PublisherBuilder().BuildAsync().ConfigureAwait(false);

            _consumer = await _configuration.Connection.ConsumerBuilder().MessageHandler(async (context, request) =>
                {
                    await context.AcceptAsync().ConfigureAwait(false);
                    if (_configuration.Handler != null)
                    {
                        await _configuration.Handler(new RpcServerContext(), request).ConfigureAwait(false);
                    }
                })
                .Queue(_configuration.RequestQueue).BuildAndStartAsync()
                .ConfigureAwait(false);

            await base.OpenAsync().ConfigureAwait(false);
        }

        private class RpcServerContext : IRpcServer.IContext
        {
            public IMessage Message(object body) => new AmqpMessage(body);
        }

        public override Task CloseAsync() => throw new System.NotImplementedException();
    }
}

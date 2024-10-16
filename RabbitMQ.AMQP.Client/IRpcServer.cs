using System;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    public delegate Task<IMessage> RpcHandler(IRpcServer.IContext context, IMessage request);

    public interface IRpcServerBuilder
    {
        /// <summary>
        /// The queue from which requests are consumed.
        /// The client sends requests to this queue and the server consumes them.
        /// </summary>
        /// <param name="requestQueue"></param>
        /// <returns></returns>
        IRpcServerBuilder RequestQueue(string requestQueue);
        IRpcServerBuilder RequestQueue(IQueueSpecification requestQueue);

        /// <summary>
        /// Extracts the correlation id from the request message.
        /// each message has a correlation id that is used to match the request with the response.
        /// There are default implementations for the correlation id extractor.
        /// With this method, you can provide a custom implementation.
        /// </summary>
        /// <param name="correlationIdExtractor"></param>
        /// <returns></returns>

        IRpcServerBuilder CorrelationIdExtractor(Func<IMessage, object>? correlationIdExtractor);

        /// <summary>
        /// Post processes the reply message before sending it to the client.
        /// The object parameter is the correlation id extracted from the request message.
        /// There are default implementations for the reply post processor that use the correlationId() field
        /// to set the correlation id of the reply message.
        /// With this method, you can provide a custom implementation.
        /// </summary>
        /// <param name="replyPostProcessor"></param>
        /// <returns></returns>
        IRpcServerBuilder ReplyPostProcessor(Func<IMessage, object, IMessage>? replyPostProcessor);

        /// <summary>
        /// Handle the request message and return the reply message.
        /// </summary>
        /// <param name="handler"></param>
        /// <returns></returns>
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

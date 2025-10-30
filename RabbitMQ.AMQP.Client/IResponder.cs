// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{

    /// <summary>
    /// IRpcServerBuilder is the interface for creating an RPC server.
    /// The RPC server consumes requests from a queue and sends replies to a reply queue.
    /// See also <seealso cref="IResponder"/>  and <seealso cref="IRequesterBuilder"/>
    /// </summary>
    public interface IResponderBuilder
    {
        /// <summary>
        /// The queue from which requests are consumed.
        /// The client sends requests to this queue and the server consumes them.
        /// </summary>
        /// <param name="requestQueue"></param>
        /// <returns></returns>
        IResponderBuilder RequestQueue(string requestQueue);
        IResponderBuilder RequestQueue(IQueueSpecification requestQueue);

        /// <summary>
        /// Extracts the correlation id from the request message.
        /// each message has a correlation id that is used to match the request with the response.
        /// There are default implementations for the correlation id extractor.
        /// With this method, you can provide a custom implementation.
        /// </summary>
        /// <param name="correlationIdExtractor"></param>
        /// <returns></returns>

        IResponderBuilder CorrelationIdExtractor(Func<IMessage, object>? correlationIdExtractor);

        /// <summary>
        /// Post processes the reply message before sending it to the client.
        /// The object parameter is the correlation id extracted from the request message.
        /// There are default implementations for the reply post processor that use the correlationId() field
        /// to set the correlation id of the reply message.
        /// With this method, you can provide a custom implementation.
        /// </summary>
        /// <param name="replyPostProcessor"></param>
        /// <returns></returns>
        IResponderBuilder ReplyPostProcessor(Func<IMessage, object, IMessage>? replyPostProcessor);

        /// <summary>
        /// Handle the request message and return the reply message.
        /// </summary>
        /// <param name="handler"></param>
        /// <returns></returns>
        IResponderBuilder Handler(RpcHandler handler);

        /// <summary>
        /// Build and return the RPC server.
        /// </summary>
        /// <returns></returns>
        Task<IResponder> BuildAsync();
    }

    /// <summary>
    /// Event handler for handling RPC requests.
    /// </summary>
    // TODO cancellation token
    public delegate Task<IMessage> RpcHandler(IResponder.IContext context, IMessage request);

    /// <summary>
    /// IRpcServer interface for creating an RPC server.
    /// The RPC is simulated by sending a request message and receiving a reply message.
    /// Where the client sends the queue where wants to receive the reply.
    /// RPC client ---> request queue ---> RPC server ---> reply queue ---> RPC client
    /// See also <seealso cref="IRequester"/>
    /// </summary>
    public interface IResponder : ILifeCycle
    {

        public interface IContext
        {
            IMessage Message(byte[] body);
            IMessage Message(string body);
        }
    }
}

// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{

    public interface IRpcClientAddressBuilder : IAddressBuilder<IRpcClientAddressBuilder>
    {
        IRpcClientBuilder RpcClient();
    }

    /// <summary>
    /// IRpcClientBuilder is the interface for creating an RPC client.
    /// See also <seealso cref="IRpcClient"/> and <seealso cref="IRpcServerBuilder"/>
    /// </summary>
    public interface IRpcClientBuilder
    {
        /// <summary>
        /// Request address where the client sends requests.
        /// The server consumes requests from this address.
        /// </summary>
        /// <returns></returns>
        IRpcClientAddressBuilder RequestAddress();

        /// <summary>
        /// The queue from which requests are consumed.
        /// if not set the client will create a temporary queue.
        /// </summary>
        /// <param name="replyToQueueName"> The queue name</param>
        /// <returns></returns>
        IRpcClientBuilder ReplyToQueue(string replyToQueueName);

        IRpcClientBuilder ReplyToQueue(IQueueSpecification replyToQueue);

        /// <summary>
        /// Extracts the correlation id from the request message.
        /// each message has a correlation id that is used to match the request with the response.
        /// There are default implementations for the correlation id extractor.
        /// With this method, you can provide a custom implementation.
        /// </summary>
        /// <param name="correlationIdExtractor"></param>
        /// <returns></returns>
        IRpcClientBuilder CorrelationIdExtractor(Func<IMessage, object>? correlationIdExtractor);

        /// <summary>
        /// Post processes the reply message before sending it to the server.
        /// The object parameter is the correlation id extracted from the request message.
        /// There are default implementations for the reply post processor that use the correlationId() field
        /// to set the correlation id of the reply message.
        /// With this method, you can provide a custom implementation.
        /// </summary>
        /// <param name="requestPostProcessor"></param>
        /// <returns></returns>
        IRpcClientBuilder RequestPostProcessor(Func<IMessage, object, IMessage>? requestPostProcessor);

        /// <summary>
        /// Client and Server must agree on the correlation id.
        /// The client will provide the correlation id to send to the server.
        /// If the default correlation id is not suitable, you can provide a custom correlation id supplier.
        /// Be careful to provide a unique correlation id for each request. 
        /// </summary>
        /// <param name="correlationIdSupplier"></param>
        /// <returns></returns>

        IRpcClientBuilder CorrelationIdSupplier(Func<object>? correlationIdSupplier);

        /// <summary>
        /// The time to wait for a reply from the server.
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        IRpcClientBuilder Timeout(TimeSpan timeout);
        /// <summary>
        ///  Build and return the RPC client.
        /// </summary>
        /// <returns></returns>
        Task<IRpcClient> BuildAsync();
    }

    /// <summary>
    ///  IRpcClient is the interface for an RPC client.
    /// See also <seealso cref="IRpcServer"/> and <seealso cref="IRpcClientBuilder"/>
    /// </summary>
    public interface IRpcClient : ILifeCycle
    {
        /// <summary>
        /// PublishAsync sends a request message to the server and blocks the thread until the response is received.
        /// The PublishAsync is thread-safe and can be called from multiple threads.
        /// The Function returns the response message.
        /// If the server does not respond within the timeout, the function throws a TimeoutException.
        /// </summary>
        /// <param name="message"> The request message</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns></returns>
        Task<IMessage> PublishAsync(IMessage message, CancellationToken cancellationToken = default);
    }
}

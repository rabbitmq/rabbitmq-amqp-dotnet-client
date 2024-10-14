using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Rpc
{
    public class RpcServerTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
    {
        [Fact]
        public async Task MockRpcServerPingPong()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            await _management.Queue(_queueName).Exclusive(true).AutoDelete(true).DeclareAsync();
            TaskCompletionSource<IMessage> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            IRpcServer rpcServer = await _connection.RpcServerBuilder().Handler((context, request) =>
            {
                var m = context.Message(request.Body()).MessageId("pong_from_the_server");
                tcs.SetResult(m);
                return Task.FromResult(m);
            }).RequestQueue(_queueName).BuildAsync();
            Assert.NotNull(rpcServer);
            IPublisher p = await _connection.PublisherBuilder().Queue(_queueName).BuildAsync();

            await p.PublishAsync(new AmqpMessage("test"));
            IMessage m = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("test", m.Body());
            Assert.Equal("pong_from_the_server", m.MessageId());
            await rpcServer.CloseAsync();
        }

        [Fact]
        public async Task RpcServerValidateStateChange()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            List<(State, State)> states = [];
            await _management.Queue(_queueName).Exclusive(true).AutoDelete(true).DeclareAsync();
            TaskCompletionSource<int> tsc = new(TaskCreationOptions.RunContinuationsAsynchronously);
            IRpcServer rpcServer = await _connection.RpcServerBuilder().Handler((context, request) =>
            {
                var m = context.Message(request.Body());
                return Task.FromResult(m);
            }).RequestQueue(_queueName).BuildAsync();
            rpcServer.ChangeState += (sender, fromState, toState, e) =>
            {
                states.Add((fromState, toState));
                if (states.Count == 2)
                {
                    tsc.SetResult(states.Count);
                }
            };
            Assert.NotNull(rpcServer);
            await rpcServer.CloseAsync();
            int count = await tsc.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(2, count);
            Assert.Equal(State.Open, states[0].Item1);
            Assert.Equal(State.Closing, states[0].Item2);
            Assert.Equal(State.Closing, states[1].Item1);
            Assert.Equal(State.Closed, states[1].Item2);
        }

        /// <summary>
        /// Simulate RPC communication with a publisher
        /// </summary>
        [Fact]
        public async Task SimulateRpcCommunicationWithAPublisherShouldSuccess()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            string requestQueue = _queueName;
            await _management.Queue(requestQueue).Exclusive(true).AutoDelete(true).DeclareAsync();
            IRpcServer rpcServer = await _connection.RpcServerBuilder().Handler((context, request) =>
            {
                var reply = context.Message(request.Body()).MessageId("pong_from_the_server");
                return Task.FromResult(reply);
            }).RequestQueue(requestQueue).BuildAsync();

            Assert.NotNull(rpcServer);
            string queueReplyTo = $"queueReplyTo-{Now}";
            IQueueSpecification spec = _management.Queue(queueReplyTo).Exclusive(true).AutoDelete(true);
            await spec.DeclareAsync();
            TaskCompletionSource<IMessage> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

            IConsumer consumer = await _connection.ConsumerBuilder().Queue(queueReplyTo).MessageHandler(
                async (context, message) =>
                {
                    await context.AcceptAsync();
                    tcs.SetResult(message);
                }).BuildAndStartAsync();

            IPublisher publisher = await _connection.PublisherBuilder().Queue(requestQueue).BuildAsync();
            Assert.NotNull(publisher);
            AddressBuilder addressBuilder = new();

            IMessage message = new AmqpMessage("test").ReplyTo(addressBuilder.Queue(queueReplyTo).Address());
            PublishResult pr = await publisher.PublishAsync(message);
            Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);

            IMessage m = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("test", m.Body());
            Assert.Equal("pong_from_the_server", m.MessageId());

            await spec.DeleteAsync();
            await rpcServer.CloseAsync();
            await consumer.CloseAsync();
            await publisher.CloseAsync();
        }

        /// <summary>
        /// In this test the client has to create a reply queue since is not provided by the user
        /// with the ReplyToQueue method
        /// </summary>
        [Fact]
        public async Task RpcServerClientPingPongWithDefault()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            string requestQueue = _queueName;
            await _management.Queue(requestQueue).Exclusive(true).AutoDelete(true).DeclareAsync();
            IRpcServer rpcServer = await _connection.RpcServerBuilder().Handler((context, request) =>
            {
                var reply = context.Message("pong");
                return Task.FromResult(reply);
            }).RequestQueue(_queueName).BuildAsync();
            Assert.NotNull(rpcServer);

            IRpcClient rpcClient = await _connection.RpcClientBuilder().RequestAddress()
                .Queue(requestQueue)
                .RpcClient()
                .BuildAsync();

            IMessage message = new AmqpMessage("ping");

            IMessage response = await rpcClient.PublishAsync(message);
            Assert.Equal("pong", response.Body());
            await rpcClient.CloseAsync();
            await rpcServer.CloseAsync();
        }

        /// <summary>
        /// In this test the client has to use the ReplyToQueue provided by the user
        /// </summary>
        [Fact]
        public async Task RpcServerClientPingPongWithCustomReplyToQueueAndCorrelationIdSupplier()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            string requestQueue = _queueName;
            await _management.Queue(requestQueue).Exclusive(true).AutoDelete(true).DeclareAsync();
            IRpcServer rpcServer = await _connection.RpcServerBuilder().Handler((context, request) =>
                {
                    var reply = context.Message("pong");
                    return Task.FromResult(reply);
                }).RequestQueue(_queueName)
                .BuildAsync();
            Assert.NotNull(rpcServer);

            // custom replyTo queue
            IQueueInfo replyTo =
                await _management.Queue($"replyTo-{Now}").Exclusive(true).AutoDelete(true).DeclareAsync();

            // custom correlationId supplier
            const string correlationId = "my-correlation-id";

            IRpcClient rpcClient = await _connection.RpcClientBuilder().RequestAddress()
                .Queue(requestQueue)
                .RpcClient()
                .CorrelationIdSupplier(() => correlationId)
                .CorrelationIdExtractor(message => message.CorrelationId())
                .ReplyToQueue(replyTo.Name())
                .BuildAsync();

            IMessage message = new AmqpMessage("ping");

            IMessage response = await rpcClient.PublishAsync(message);
            Assert.Equal("pong", response.Body());
            Assert.Equal(correlationId, response.CorrelationId());
            await rpcClient.CloseAsync();
            await rpcServer.CloseAsync();
        }
    }
}

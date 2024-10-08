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
                var m = context.Message(request.Body()).MessageId("pong_from_the_server");
                return Task.FromResult(m);
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
    }
}

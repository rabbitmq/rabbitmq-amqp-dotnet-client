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
        [Fact(Skip = "ci fail")]
        public async Task MockRpcServerPingPong()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            await _management.Queue(_queueName).Exclusive(true).AutoDelete(true).DeclareAsync();
            TaskCompletionSource<IMessage> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            IRpcServer rpcServer = await _connection.RpcServerBuilder().Handler((context, request) =>
            {
                var reply = context.Message("pong");
                tcs.SetResult(reply);
                return Task.FromResult(reply);
            }).RequestQueue(_queueName).BuildAsync();
            Assert.NotNull(rpcServer);
            IPublisher p = await _connection.PublisherBuilder().Queue(_queueName).BuildAsync();

            await p.PublishAsync(new AmqpMessage("test"));
            IMessage m = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("pong", m.Body());
            await rpcServer.CloseAsync();
        }

        [Fact(Skip = "ci fail")]
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
        [Fact(Skip = "ci fail")]
        public async Task SimulateRpcCommunicationWithAPublisherShouldSuccess()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            string requestQueue = _queueName;
            await _management.Queue(requestQueue).Exclusive(true).AutoDelete(true).DeclareAsync();
            IRpcServer rpcServer = await _connection.RpcServerBuilder().Handler((context, request) =>
            {
                var reply = context.Message("pong");
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
            Assert.Equal("pong", m.Body());

            await rpcServer.CloseAsync();
            await consumer.CloseAsync();
            await publisher.CloseAsync();
        }

        /// <summary>
        /// In this test the client has to create a reply queue since is not provided by the user
        /// with the ReplyToQueue method
        /// </summary>
        [Fact(Skip = "ci fail")]
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
        [Fact(Skip = "ci fail")]
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

        /// <summary>
        /// This test combine all the features with the overriding of the request and response post processor
        /// the correlation id supplier and the extraction of the correlationId.
        /// Here the client uses the replyTo queue provided by the user and the correlationId supplier
        /// the field "Subject" is used as correlationId
        /// The server uses the field "GroupId" as correlationId
        /// Both use the extraction correlationId to get the correlationId
        ///
        /// The fields "Subject" and "GroupId" are used ONLY for test.
        /// You should not use these fields for this purpose.
        /// </summary>
        /// <exception cref="InvalidOperationException"></exception>

        [Fact(Skip = "ci fail")]
        public async Task RpcServerClientOverridingTheRequestAndResponsePostProcessor()
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
                //come from the client
                .CorrelationIdExtractor(message => message.Subject())
                //  replace the correlation id location with GroupId
                .ReplyPostProcessor((reply, replyCorrelationId) => reply.GroupId(
                    replyCorrelationId.ToString() ?? throw new InvalidOperationException()))
                .BuildAsync();
            Assert.NotNull(rpcServer);

            IQueueInfo replyTo =
                await _management.Queue($"replyTo-{Now}").Exclusive(true).AutoDelete(true).DeclareAsync();

            // custom correlationId supplier
            const string correlationId = "my-correlation-id";
            int correlationIdCounter = 0;

            IRpcClient rpcClient = await _connection.RpcClientBuilder().RequestAddress()
                .Queue(requestQueue)
                .RpcClient()
                .ReplyToQueue(replyTo.Name())
                // replace the correlation id creation with a custom function
                .CorrelationIdSupplier(() => $"{correlationId}_{Interlocked.Increment(ref correlationIdCounter)}")
                // The server will reply with the correlation id in the groupId
                // This is only for testing. You should not use the groupId for this. 
                .CorrelationIdExtractor(message => message.GroupId())
                // The client will use Subject to store the correlation id
                // this is only for testing. You should not use Subject for this.
                .RequestPostProcessor((request, requestCorrelationId)
                    => request.ReplyTo(AddressBuilderHelper.AddressBuilder().Queue(replyTo.Name()).Address())
                        .Subject(requestCorrelationId.ToString() ?? throw new InvalidOperationException()))
                .BuildAsync();

            IMessage message = new AmqpMessage("ping");

            int i = 1;
            while (i < 30)
            {
                IMessage response = await rpcClient.PublishAsync(message);
                Assert.Equal("pong", response.Body());
                // the server replies with the correlation id in the GroupId field
                Assert.Equal($"{correlationId}_{i}", response.GroupId());
                i++;
            }

            await rpcClient.CloseAsync();
            await rpcServer.CloseAsync();
        }
    }
}

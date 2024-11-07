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
                var reply = context.Message("pong");
                return Task.FromResult(reply);
            }).RequestQueue(requestQueue).BuildAsync();

            Assert.NotNull(rpcServer);
            string queueReplyTo = $"queueReplyTo-{Now}";
            IQueueSpecification spec = _management.Queue(queueReplyTo).Exclusive(true).AutoDelete(true);
            await spec.DeclareAsync();
            TaskCompletionSource<IMessage> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

            IConsumer consumer = await _connection.ConsumerBuilder().Queue(queueReplyTo).MessageHandler(
                (context, message) =>
                {
                    context.Accept();
                    tcs.SetResult(message);
                    return Task.CompletedTask;
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

        /// <summary>
        /// This test combine all the features with the overriding of the request and response post processor
        /// the correlation id supplier and the extraction of the correlationId.
        /// Here the client uses the replyTo queue provided by the user and the correlationId supplier
        ///
        /// </summary>
        /// <exception cref="InvalidOperationException"></exception>
        [Fact]
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
                .CorrelationIdExtractor(message => message.Property("correlationId"))
                //  replace the correlation id location with Application properties
                .ReplyPostProcessor((reply, replyCorrelationId) => reply.Property("correlationId",
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
                // The server will reply with the correlation id in application properties
                .CorrelationIdExtractor(message => message.Property("correlationId"))
                // The client will use application properties to set the correlation id
                .RequestPostProcessor((request, requestCorrelationId)
                    => request.ReplyTo(AddressBuilderHelper.AddressBuilder().Queue(replyTo.Name()).Address())
                        .Property("correlationId",
                            requestCorrelationId.ToString() ?? throw new InvalidOperationException()))
                .BuildAsync();

            IMessage message = new AmqpMessage("ping");

            int i = 1;
            while (i < 30)
            {
                IMessage response = await rpcClient.PublishAsync(message);
                Assert.Equal("pong", response.Body());
                // the server replies with the correlation id in the application properties
                Assert.Equal($"{correlationId}_{i}", response.Property("correlationId"));
                Assert.Equal($"{correlationId}_{i}", response.Properties()["correlationId"]);
                Assert.Single(response.Properties());
                i++;
            }

            await rpcClient.CloseAsync();
            await rpcServer.CloseAsync();
        }

        [Fact]
        public async Task RpcClientMultiThreadShouldBeSafe()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            string requestQueue = _queueName;

            await _management.Queue(requestQueue).Exclusive(true).AutoDelete(true).DeclareAsync();
            const int messagesToSend = 99;
            TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            List<IMessage> messagesReceived = [];
            IRpcServer rpcServer = await _connection.RpcServerBuilder().Handler((context, request) =>
            {
                try
                {
                    var reply = context.Message("pong");
                    messagesReceived.Add(request);
                    return Task.FromResult(reply);
                }
                finally
                {
                    if (messagesReceived.Count == messagesToSend)
                    {
                        tcs.SetResult(true);
                    }
                }
            }).RequestQueue(requestQueue).BuildAsync();

            Assert.NotNull(rpcServer);

            IRpcClient rpcClient = await _connection.RpcClientBuilder().RequestAddress()
                .Queue(requestQueue)
                .RpcClient()
                .BuildAsync();

            List<Task> tasks = [];

            // we simulate a multi-thread environment
            // where multiple threads send messages to the server
            // and the server replies to each message in a consistent way
            for (int i = 0; i < messagesToSend; i++)
            {
                int i1 = i;
                tasks.Add(Task.Run(async () =>
                {
                    IMessage message = new AmqpMessage("ping").Property("id", i1);
                    IMessage response = await rpcClient.PublishAsync(message);
                    Assert.Equal("pong", response.Body());
                }));
            }

            await Task.WhenAll(tasks);

            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(messagesToSend, messagesReceived.Count);

            // we don't care about the order of the messages
            // the important thing is that all the messages are received
            // and the id is the same as the one sent
            for (int i = 0; i < messagesToSend; i++)
            {
                Assert.Contains(messagesReceived, m => m.Property("id").Equals(i));
            }

            await rpcServer.CloseAsync();
            await rpcClient.CloseAsync();
        }

        /// <summary>
        /// The RPC client `PublishAsync` should raise a timeout exception if the server does not reply within the timeout
        /// </summary>
        [Fact]
        public async Task RpcClientShouldRaiseTimeoutError()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            string requestQueue = _queueName;
            await _management.Queue(requestQueue).Exclusive(true).AutoDelete(true).DeclareAsync();
            IRpcServer rpcServer = await _connection.RpcServerBuilder().Handler(async (context, request) =>
            {
                IMessage reply = context.Message("pong");
                object millisecondsToWait = request.Property("wait");
                await Task.Delay(TimeSpan.FromMilliseconds((int)millisecondsToWait));
                return reply;
            }).RequestQueue(_queueName).BuildAsync();
            Assert.NotNull(rpcServer);

            IRpcClient rpcClient = await _connection.RpcClientBuilder().RequestAddress()
                .Queue(requestQueue)
                .RpcClient()
                .Timeout(TimeSpan.FromMilliseconds(300))
                .BuildAsync();

            IMessage reply = await rpcClient.PublishAsync(
                new AmqpMessage("ping").Property("wait", 1));
            Assert.Equal("pong", reply.Body());

            await Assert.ThrowsAsync<TimeoutException>(() => rpcClient.PublishAsync(
                new AmqpMessage("ping").Property("wait", 700)));

            await rpcClient.CloseAsync();
            await rpcServer.CloseAsync();
        }
    }
}

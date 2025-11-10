// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.RequesterResponser
{
    public class ResponderTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
    {
        private string _requestQueueName = string.Empty;
        private readonly string _replyToName = $"queueReplyTo-{Now}-{Guid.NewGuid()}";
        private readonly string _correlationId = $"my-correlation-id-{Guid.NewGuid()}";

        public override async Task InitializeAsync()
        {
            await base.InitializeAsync();

            Assert.NotNull(_management);

            IQueueInfo requestQueueInfo = await _management.Queue()
                .Exclusive(true)
                .AutoDelete(true)
                .DeclareAsync();

            _requestQueueName = requestQueueInfo.Name();
        }

        [Fact]
        public async Task MockResponderPingPong()
        {
            Assert.NotNull(_connection);
            TaskCompletionSource<IMessage> tcs = CreateTaskCompletionSource<IMessage>();

            IResponder responder = await _connection.ResponderBuilder()
                .Handler(Handler)
                .RequestQueue(_requestQueueName)
                .BuildAsync();

            IPublisher p = await _connection.PublisherBuilder()
                .Queue(_requestQueueName)
                .BuildAsync();

            await p.PublishAsync(new AmqpMessage("test"));
            IMessage m = await WhenTcsCompletes(tcs);
            Assert.Equal("pong", m.BodyAsString());
            await responder.CloseAsync();
            return;

            Task<IMessage> Handler(IResponder.IContext context, IMessage request)
            {
                IMessage reply = context.Message("pong");
                tcs.SetResult(reply);
                return Task.FromResult(reply);
            }
        }

        [Fact]
        public async Task ResponderValidateStateChange()
        {
            Assert.NotNull(_connection);

            List<(State, State)> states = [];
            TaskCompletionSource<int> tcs = CreateTaskCompletionSource<int>();

            IResponder responder = await _connection.ResponderBuilder()
                .Handler(Handler)
                .RequestQueue(_requestQueueName)
                .BuildAsync();

            responder.ChangeState += (sender, fromState, toState, e) =>
            {
                states.Add((fromState, toState));
                if (states.Count == 2)
                {
                    tcs.SetResult(states.Count);
                }
            };

            await responder.CloseAsync();

            int count = await WhenTcsCompletes(tcs);
            Assert.Equal(2, count);
            Assert.Equal(State.Open, states[0].Item1);
            Assert.Equal(State.Closing, states[0].Item2);
            Assert.Equal(State.Closing, states[1].Item1);
            Assert.Equal(State.Closed, states[1].Item2);
            return;

            static Task<IMessage> Handler(IResponder.IContext context, IMessage request)
            {
                IMessage m = context.Message(request.Body());
                return Task.FromResult(m);
            }
        }

        /// <summary>
        /// Simulate RPC communication with a publisher
        /// </summary>
        [Fact]
        public async Task SimulateRpcCommunicationWithAPublisherShouldSuccess()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IResponder responder = await _connection.ResponderBuilder()
                .Handler(PongResponderHandler)
                .RequestQueue(_requestQueueName)
                .BuildAsync();

            IQueueSpecification replyQueueSpec = _management.Queue(_replyToName)
                .Exclusive(true)
                .AutoDelete(true);
            await replyQueueSpec.DeclareAsync();

            TaskCompletionSource<IMessage> tcs = CreateTaskCompletionSource<IMessage>();

            IConsumer consumer = await _connection.ConsumerBuilder()
                .Queue(replyQueueSpec)
                .MessageHandler(MessageHandler)
                .BuildAndStartAsync();

            IPublisher publisher = await _connection.PublisherBuilder()
                .Queue(_requestQueueName)
                .BuildAsync();

            AddressBuilder addressBuilder = new();
            string replyToAddress = addressBuilder.Queue(replyQueueSpec).Address();
            IMessage message = new AmqpMessage("test").ReplyTo(replyToAddress);
            PublishResult pr = await publisher.PublishAsync(message);
            Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);

            IMessage m = await WhenTcsCompletes(tcs);
            Assert.Equal("pong", m.BodyAsString());

            await responder.CloseAsync();
            await consumer.CloseAsync();
            await publisher.CloseAsync();
            return;

            Task MessageHandler(IContext context, IMessage msg)
            {
                context.Accept();
                tcs.SetResult(msg);
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// In this test the client has to create a reply queue since is not provided by the user
        /// with the ReplyToQueue method
        /// </summary>
        [Fact]
        public async Task ResponderRequesterPingPongWithDefault()
        {
            Assert.NotNull(_connection);

            IResponder responder = await _connection.ResponderBuilder()
                .Handler(PongResponderHandler)
                .RequestQueue(_requestQueueName)
                .BuildAsync();

            IRequester requester = await _connection.RequesterBuilder()
                .RequestAddress()
                .Queue(_requestQueueName)
                .Requester()
                .BuildAsync();

            IMessage message = new AmqpMessage("ping");

            IMessage response = await requester.PublishAsync(message);
            Assert.Equal("pong", response.BodyAsString());

            Assert.Contains(
                _connection is AmqpConnection { _featureFlags.IsDirectReplyToSupported: true }
                    ? "amq.rabbitmq.reply-to"
                    : "client.gen-", requester.GetReplyToQueue());

            await requester.CloseAsync();
            await responder.CloseAsync();
        }

        /// <summary>
        /// In this test the Requester has to use the ReplyToQueue provided by the user
        /// </summary>
        [Fact]
        public async Task ResponderRequesterPingPongWithCustomReplyToQueueAndCorrelationIdSupplier()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IResponder responder = await _connection.ResponderBuilder()
                .Handler(PongResponderHandler)
                .RequestQueue(_requestQueueName)
                .BuildAsync();

            IQueueInfo replyTo = await _management.Queue(_replyToName)
                .Exclusive(true)
                .AutoDelete(true)
                .DeclareAsync();

            IRequester requester = await _connection.RequesterBuilder()
                .RequestAddress()
                .Queue(_requestQueueName)
                .Requester()
                .CorrelationIdSupplier(() => _correlationId)
                .CorrelationIdExtractor(message => message.CorrelationId())
                .ReplyToQueue(replyTo.Name())
                .BuildAsync();

            IMessage message = new AmqpMessage("ping");

            IMessage response = await requester.PublishAsync(message);
            Assert.Equal("pong", response.BodyAsString());
            Assert.Equal(_correlationId, response.CorrelationId());
            Assert.Contains(replyTo.Name(), requester.GetReplyToQueue());

            await requester.CloseAsync();
            await responder.CloseAsync();
        }

        /// <summary>
        /// This test combine all the features with the overriding of the request and response post processor
        /// the correlation id supplier and the extraction of the correlationId.
        /// Here the client uses the replyTo queue provided by the user and the correlationId supplier
        ///
        /// </summary>
        /// <exception cref="InvalidOperationException"></exception>
        [Fact]
        public async Task ResponderRequesterOverridingTheRequestAndResponsePostProcessor()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IResponder responder = await _connection.ResponderBuilder()
                .Handler(PongResponderHandler)
                .RequestQueue(_requestQueueName)
                .CorrelationIdExtractor(message => message.Property("correlationId"))
                .ReplyPostProcessor((reply, replyCorrelationId) => reply.Property("correlationId",
                    replyCorrelationId.ToString() ?? throw new InvalidOperationException()))
                .BuildAsync();

            IQueueInfo replyTo = await _management.Queue(_replyToName)
                .Exclusive(true)
                .AutoDelete(true)
                .DeclareAsync();

            int correlationIdCounter = 0;

            IRequester requester = await _connection.RequesterBuilder()
                .RequestAddress()
                .Queue(_requestQueueName)
                .Requester()
                .ReplyToQueue(replyTo.Name())
                // replace the correlation id creation with a custom function
                .CorrelationIdSupplier(() => $"{_correlationId}_{Interlocked.Increment(ref correlationIdCounter)}")
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
                IMessage response = await requester.PublishAsync(message);
                Assert.Equal("pong", response.BodyAsString());
                // the server replies with the correlation id in the application properties
                Assert.Equal($"{_correlationId}_{i}", response.Property("correlationId"));
                Assert.Equal($"{_correlationId}_{i}", response.Properties()["correlationId"]);
                Assert.Single(response.Properties());
                i++;
            }

            await requester.CloseAsync();
            await responder.CloseAsync();
        }

        [Fact]
        public async Task RequesterMultiThreadShouldBeSafe()
        {
            Assert.NotNull(_connection);
            const int messagesToSend = 99;

            TaskCompletionSource<bool> tcs = CreateTaskCompletionSource();
            List<IMessage> messagesReceived = [];

            Task<IMessage> ResponderHandler(IResponder.IContext context, IMessage request)
            {
                try
                {
                    IMessage reply = context.Message("pong");
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
            }

            IResponder responder = await _connection.ResponderBuilder()
                .Handler(ResponderHandler)
                .RequestQueue(_requestQueueName)
                .BuildAsync();

            IRequester requester = await _connection.RequesterBuilder().RequestAddress()
                .Queue(_requestQueueName)
                .Requester()
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
                    IMessage response = await requester.PublishAsync(message);
                    Assert.Equal("pong", response.BodyAsString());
                }));
            }

            await WhenAllComplete(tasks);

            await WhenTcsCompletes(tcs);

            Assert.Equal(messagesToSend, messagesReceived.Count);

            // we don't care about the order of the messages
            // the important thing is that all the messages are received
            // and the id is the same as the one sent
            for (int i = 0; i < messagesToSend; i++)
            {
                Assert.Contains(messagesReceived, m => m.Property("id").Equals(i));
            }

            await responder.CloseAsync();
            await requester.CloseAsync();
        }

        /// <summary>
        /// The Requester `PublishAsync` should raise a timeout exception if the server does not reply within the timeout
        /// </summary>
        [Fact]
        public async Task RequesterShouldRaiseTimeoutError()
        {
            Assert.NotNull(_connection);

            static async Task<IMessage> RequesterHandler(IResponder.IContext context, IMessage request)
            {
                IMessage reply = context.Message("pong");
                object millisecondsToWait = request.Property("wait");
                await Task.Delay(TimeSpan.FromMilliseconds((int)millisecondsToWait));
                return reply;
            }

            IResponder responder = await _connection.ResponderBuilder()
                .Handler(RequesterHandler)
                .RequestQueue(_requestQueueName)
                .BuildAsync();

            IRequester requester = await _connection.RequesterBuilder()
                .RequestAddress()
                .Queue(_requestQueueName)
                .Requester()
                .Timeout(TimeSpan.FromMilliseconds(300))
                .BuildAsync();

            IMessage msg = new AmqpMessage("ping").Property("wait", 1);
            IMessage reply = await requester.PublishAsync(msg);
            Assert.Equal("pong", reply.BodyAsString());

            await Assert.ThrowsAsync<TimeoutException>(() => requester.PublishAsync(
                new AmqpMessage("ping").Property("wait", 700)));

            await requester.CloseAsync();
            await responder.CloseAsync();
        }

        private static Task<IMessage> PongResponderHandler(IResponder.IContext context, IMessage request)
        {
            IMessage reply = context.Message("pong");
            return Task.FromResult(reply);
        }
    }
}

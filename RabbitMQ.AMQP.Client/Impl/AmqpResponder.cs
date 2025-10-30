// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading.Tasks;
using Amqp;

namespace RabbitMQ.AMQP.Client.Impl
{
    public class RpcConfiguration
    {
        public AmqpConnection Connection { get; set; } = null!;
        public RpcHandler? Handler { get; set; }
        public string RequestQueue { get; set; } = "";
        public Func<IMessage, object>? CorrelationIdExtractor { get; set; }
        public Func<IMessage, object, IMessage>? ReplyPostProcessor { get; set; }
    }

    /// <summary>
    ///  AmqpRpcServerBuilder is a builder for creating an AMQP RPC server.
    /// </summary>
    public class AmqpResponderBuilder : IResponderBuilder
    {
        readonly RpcConfiguration _configuration = new();

        public AmqpResponderBuilder(AmqpConnection connection)
        {
            _configuration.Connection = connection;
        }

        public IResponderBuilder RequestQueue(string requestQueue)
        {
            _configuration.RequestQueue = requestQueue;
            return this;
        }

        public IResponderBuilder RequestQueue(IQueueSpecification requestQueue)
        {
            _configuration.RequestQueue = requestQueue.QueueName;
            return this;
        }

        public IResponderBuilder CorrelationIdExtractor(Func<IMessage, object>? correlationIdExtractor)
        {
            _configuration.CorrelationIdExtractor = correlationIdExtractor;
            return this;
        }

        public IResponderBuilder ReplyPostProcessor(Func<IMessage, object, IMessage>? replyPostProcessor)
        {
            _configuration.ReplyPostProcessor = replyPostProcessor;
            return this;
        }

        public IResponderBuilder Handler(RpcHandler handler)
        {
            _configuration.Handler = handler;
            return this;
        }

        public async Task<IResponder> BuildAsync()
        {
            AmqpResponder amqpResponder = new(_configuration);
            await amqpResponder.OpenAsync().ConfigureAwait(false);
            return amqpResponder;
        }
    }

    /// <summary>
    /// AmqpRpcServer implements the <see cref="IResponder"/> interface.
    /// With the RpcClient you can create an RPC communication over AMQP 1.0.
    /// </summary>
    public class AmqpResponder : AbstractLifeCycle, IResponder
    {
        private readonly RpcConfiguration _configuration;
        private IPublisher? _publisher = null;
        private IConsumer? _consumer = null;

        private async Task SendReply(IMessage reply)
        {
            if (_publisher != null)
            {
                PublishResult pr = await _publisher.PublishAsync(reply).ConfigureAwait(false);
                if (pr.Outcome.State != OutcomeState.Accepted)
                {
                    Trace.WriteLine(TraceLevel.Error, "Failed to send reply");
                }
            }
        }

        private object ExtractCorrelationId(IMessage message)
        {
            object corr = message.MessageId();
            if (_configuration.CorrelationIdExtractor != null)
            {
                corr = _configuration.CorrelationIdExtractor(message);
            }

            return corr;
        }

        private IMessage ReplyPostProcessor(IMessage reply, object correlationId)
        {
            return _configuration.ReplyPostProcessor != null
                ? _configuration.ReplyPostProcessor(reply, correlationId)
                : reply.CorrelationId(correlationId);
        }

        public AmqpResponder(RpcConfiguration configuration)
        {
            _configuration = configuration;
        }

        public override async Task OpenAsync()
        {
            _publisher = await _configuration.Connection.PublisherBuilder().BuildAsync().ConfigureAwait(false);

            _consumer = await _configuration.Connection.ConsumerBuilder().MessageHandler(async (context, request) =>
                {
                    context.Accept();
                    if (_configuration.Handler != null)
                    {
                        IMessage reply = await _configuration.Handler(new RpcServerContext(), request)
                            .ConfigureAwait(false);

                        if (request.ReplyTo() != "")
                        {
                            reply.To(request.ReplyTo());
                        }
                        else
                        {
                            Trace.WriteLine(TraceLevel.Error, "[RPC server] No reply-to address in request");
                        }

                        object correlationId = ExtractCorrelationId(request);
                        reply = ReplyPostProcessor(reply, correlationId);
                        await Utils.WaitWithBackOffUntilFuncAsync(async () =>
                            {
                                try
                                {
                                    await SendReply(reply).ConfigureAwait(false);
                                    return true;
                                }
                                catch (Exception e)
                                {
                                    Trace.WriteLine(TraceLevel.Error,
                                        $"[RPC server] Failed to send reply: {e.Message}");
                                    return false;
                                }
                            },
                            (success, span) =>
                            {
                                if (!success)
                                {
                                    Trace.WriteLine(TraceLevel.Error, $"Failed to send reply, retrying in {span}");
                                }
                            }, 5).ConfigureAwait(false);
                    }
                })
                .Queue(_configuration.RequestQueue).BuildAndStartAsync()
                .ConfigureAwait(false);

            await base.OpenAsync().ConfigureAwait(false);
        }

        private class RpcServerContext : IResponder.IContext
        {
            public IMessage Message(byte[] body) => new AmqpMessage(body);
            public IMessage Message(string body) => new AmqpMessage(body);
        }

        public override async Task CloseAsync()
        {
            OnNewStatus(State.Closing, null);
            try
            {
                if (_publisher != null)
                {
                    await _publisher.CloseAsync().ConfigureAwait(false);
                }

                if (_consumer != null)
                {
                    await _consumer.CloseAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                OnNewStatus(State.Closed, null);
            }
        }
    }
}

using System;
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
        public async Task RpcServerPingPong()
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
        }
    }
}

using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Rpc
{
    public class RecoveryRpcTests(ITestOutputHelper testOutputHelper)
    {
        [Fact]
        public async Task RpcServerAndClientShouldRecoverAfterKillConnection()
        {
            string containerId = $"rpc-server-client-recovery-{DateTime.Now}";
            IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create()
                .ContainerId(containerId).RecoveryConfiguration(RecoveryConfiguration.Create().Topology(true)).Build());
            IManagement management = connection.Management();
            string rpcRequestQueueName = $"rpc-server-client-recovery-queue-request-{DateTime.Now}";
            IQueueSpecification requestQueue = management.Queue(rpcRequestQueueName)
                .Type(QueueType.CLASSIC);

            await requestQueue.DeclareAsync();
            int messagesReceived = 0;
            IRpcServer rpcServer = await connection.RpcServerBuilder()
                .RequestQueue(rpcRequestQueueName)
                .Handler(async (context, message) =>
                {
                    Interlocked.Increment(ref messagesReceived);
                    var reply = context.Message("pong");
                    return await Task.FromResult(reply);
                })
                .BuildAsync();

            string replyQueueName = $"rpc-server-client-recovery-reply-queue-{DateTime.Now}";

            IQueueSpecification clientReplyQueue = management.Queue(replyQueueName)
                .Type(QueueType.CLASSIC).AutoDelete(true).Exclusive(true);

            await clientReplyQueue.DeclareAsync();

            IRpcClient rpcClient = await
                connection.RpcClientBuilder().RequestAddress().Queue(requestQueue).RpcClient()
                    .ReplyToQueue(clientReplyQueue).BuildAsync();

            int messagesConfirmed = 0;
            for (int i = 0; i < 50; i++)
            {
                IMessage request = new AmqpMessage("ping");
                try
                {
                    IMessage response = await rpcClient.PublishAsync(request);
                    messagesConfirmed++;
                    Assert.Equal("pong", response.Body());
                }
                catch (Exception e)
                {
                    testOutputHelper.WriteLine($"Error sending message: {e.Message}");
                    await Task.Delay(700);
                }

                if (i % 25 == 0)
                {
                    await SystemUtils.WaitUntilConnectionIsKilled(containerId);
                    await Task.Delay(500);
                    await SystemUtils.WaitUntilQueueExistsAsync(clientReplyQueue.QueueName);
                }
            }

            Assert.True(messagesConfirmed > 25);
            Assert.True(messagesReceived > 25);
            await requestQueue.DeleteAsync();
            await clientReplyQueue.DeleteAsync();
            await rpcClient.CloseAsync();
            await rpcServer.CloseAsync();
        }
    }
}

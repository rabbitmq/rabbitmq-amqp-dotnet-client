// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

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
        : IntegrationTest(testOutputHelper, setupConnectionAndManagement: false)
    {
        [Fact]
        public async Task RpcServerAndClientShouldRecoverAfterKillConnection()
        {
            Assert.Null(_connection);
            Assert.Null(_management);

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
                .Handler((context, message) =>
                {
                    Interlocked.Increment(ref messagesReceived);
                    var reply = context.Message("pong");
                    return Task.FromResult(reply);
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
                catch (AmqpNotOpenException)
                {
                    await Task.Delay(700);
                }
                catch (Exception e)
                {
                    _testOutputHelper.WriteLine($"[ERROR] unexpected exception while sending message: {e.Message}");
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

// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Affinity
{
    public class AffinityTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
    {
        [Fact]
        public void CreateAffinity()
        {
            const string queueName = "testQueue";
            const Operation operation = Operation.Publish;
            IAffinity affinity = new DefaultAffinity(queueName, operation);

            Assert.Equal(queueName, affinity.Queue());
            Assert.Equal(operation, affinity.Operation());
        }

        [Fact]
        public async Task TryToFindUriNode()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            IQueueSpecification queueSpec = _management.Queue().Name(_queueName).Type(QueueType.QUORUM);
            await queueSpec.DeclareAsync();
            const Operation operation = Operation.Publish;
            IConnection connectionAffinity = await AmqpConnection.CreateAsync(ConnectionSettingsBuilder.Create()
                .Affinity(new DefaultAffinity(_queueName, operation)).Build());
            Assert.NotNull(connectionAffinity);
            await queueSpec.DeleteAsync();

        }
    }
}

// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EasyNetQ.Management.Client.Model;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;
using QueueType = RabbitMQ.AMQP.Client.QueueType;

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

        /// <summary>
        /// FindTheAffinityNode is to verify that the connection can find the node where the queue
        /// is located and connect to it when the affinity is specified.
        /// With a single node it doesn't verify much, but it at least verifies
        /// that the affinity is being used to find the node where the queue is located.
        /// </summary>
        [Fact]
        public async Task FindTheAffinityNode()
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
            await connectionAffinity.CloseAsync();
        }

        /// <summary>
        /// AffinityShouldWorkEvenTheQueueDoesNotExist is to verify that the connection can still be created even if the queue specified in the affinity does not exist.
        /// This is to ensure that the connection creation does not fail due to a non-existent queue,
        /// Affinity feature is a best-effort so if it can't find the connection to the node where the queue is located, it should not fail if the queue does not exist,
        /// as the connection can still be used for other operations that do not require the queue to exist.
        /// </summary>
        [Fact]
        public async Task AffinityShouldWorkEvenTheQueueDoesNotExist()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            const string nonExistentQueueName = "nonExistentQueue";
            const Operation operation = Operation.Publish;
            IConnection connectionAffinity = await AmqpConnection.CreateAsync(ConnectionSettingsBuilder.Create()
                .Affinity(new DefaultAffinity(nonExistentQueueName, operation)).Build());
            Assert.NotNull(connectionAffinity);
            await connectionAffinity.CloseAsync();
        }

        /// <summary>
        ///  FindTheAffinityNodeInCluster is to verify that the connection can find the node where
        /// the queue is located and connect to it when the affinity is specified in a cluster environment.
        /// That's the real use case for the affinity feature, as in a single node environment,
        /// the connection will always connect to the same node, so it doesn't verify much.
        /// to run this test, you need to start a cluster environment with 3 nodes in docker using the rabbitmq-cluster-start script
        /// provided in the repository.
        /// </summary>
        [SkippableFact]
        public async Task FindTheAffinityNodeInCluster()
        {
            // skip if not in cluster
            Skip.IfNot(IsCluster,
                "This test is only for cluster environment. make rabbitmq-cluster-start to start a cluster environment with 3 nodes in docker");

            ConnectionSettings defaultSettings = ConnectionSettingsBuilder.Create().Uris([
                new Uri("amqp://localhost:5673"),
                new Uri("amqp://localhost:5672"),
                new Uri("amqp://localhost:5674"),
            ]).Build();
            IEnvironment environment = AmqpEnvironment.Create(defaultSettings);

            IConnection connection = await environment.CreateConnectionAsync().ConfigureAwait(false);
            IManagement management = connection.Management();
            // given the cluster setting (queue_leader_locator = balanced) the queue will be created in one of the nodes,
            // we don't know which one, but it doesn't matter
            // as the affinity connection should be able to find it
            IQueueSpecification queueSpec = management.Queue(_queueName).Type(QueueType.QUORUM);
            await queueSpec.DeclareAsync().ConfigureAwait(false);

            IConnection connectionAffinity = await environment.CreateConnectionAsync(ConnectionSettingsBuilder
                // Create another Builder starting from the default settings, so it has the same list of URIs and other settings as the default one,
                // except for the affinity and URI selector that we are going to override.
                .From(defaultSettings)
                // Override the default URI selector with a custom one that implements a round-robin strategy
                // to select the next URI from the list for each new connection attempt.
                // This avoids where the default selector randomly selects the same URI.
                // Not strictly necessary, but it is a way to see another interesting feature of the client,
                // the ability to plug in custom URI selectors.
                .UriSelector(new SequentialSelector())
                // Define the affinity to the queue we just created, with the publish operation.
                .Affinity(new DefaultAffinity(_queueName, Operation.Publish)).Build()).ConfigureAwait(false);
            Assert.NotNull(connectionAffinity);

            IQueueInfo queueInfo = await connectionAffinity.Management().GetQueueInfoAsync(_queueName).ConfigureAwait(false);
            string? serverName = queueInfo.Leader();
            Assert.NotNull(serverName);
            Assert.True(connectionAffinity.Properties.TryGetValue("node", out object? nodeName) || connectionAffinity.Properties.TryGetValue("server", out nodeName));
            Assert.Equal(serverName, nodeName);
            await queueSpec.DeleteAsync().ConfigureAwait(false);
            await environment.CloseAsync().ConfigureAwait(false);

        }
    }

    internal class SequentialSelector : IUriSelector
    {
        private int _lastIndex = -1;

        public Uri Select(ICollection<Uri> uris)
        {
            if (uris == null || uris.Count == 0)
            {
                throw new ArgumentException("URIs collection cannot be null or empty.");
            }

            _lastIndex = (_lastIndex + 1) % uris.Count;
            return uris.ElementAt(_lastIndex);
        }
    }
}

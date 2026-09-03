// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.ConnectionTests;

/// <summary>
/// Transport factory cases that need a running broker. The factory dials the broker itself over the
/// loopback interface, with no proxy in the way, so the connection has to behave exactly as it does
/// on the socket the library would have opened.
/// </summary>
public class TransportFactoryBrokerTests(ITestOutputHelper testOutputHelper)
    : IntegrationTest(testOutputHelper, setupConnectionAndManagement: false)
{
    [Fact]
    public async Task MessageRoundTripsOverTheSuppliedTransport()
    {
        int transportFactoryInvocations = 0;

        ConnectionSettings connectionSettings = _connectionSettingBuilder
            .TransportFactory((host, port, cancellationToken) =>
            {
                Interlocked.Increment(ref transportFactoryInvocations);
                return DialAsync(host, port);
            })
            .Build();

        IConnection connection = await AmqpConnection.CreateAsync(connectionSettings);
        try
        {
            Assert.Equal(State.Open, connection.State);
            Assert.Equal(1, transportFactoryInvocations);

            IManagement management = connection.Management();
            IQueueSpecification queueSpecification = management.Queue(_queueName)
                .Exclusive(true)
                .AutoDelete(true);
            await queueSpecification.DeclareAsync();

            string body = $"transport-factory-{RandomString(16)}";

            IPublisher publisher = await connection.PublisherBuilder()
                .Queue(queueSpecification)
                .BuildAsync();
            try
            {
                PublishResult publishResult = await publisher.PublishAsync(new AmqpMessage(body));
                Assert.Equal(OutcomeState.Accepted, publishResult.Outcome.State);
            }
            finally
            {
                await publisher.CloseAsync();
                publisher.Dispose();
            }

            TaskCompletionSource<IMessage> receivedTcs = CreateTaskCompletionSource<IMessage>();
            IConsumer consumer = await connection.ConsumerBuilder()
                .Queue(queueSpecification)
                .MessageHandler((context, message) =>
                {
                    context.Accept();
                    receivedTcs.TrySetResult(message);
                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();
            try
            {
                IMessage received = await WhenTcsCompletes(receivedTcs);
                Assert.Equal(body, received.BodyAsString());
            }
            finally
            {
                await consumer.CloseAsync();
                consumer.Dispose();
            }

            // Publishing and consuming ran over the one transport the factory supplied. Nothing was
            // re-dialled behind the application's back.
            Assert.Equal(1, transportFactoryInvocations);
        }
        finally
        {
            await connection.CloseAsync();
            connection.Dispose();
        }
    }

    [Fact]
    public async Task TransportFactoryIsInvokedOncePerConnectionAttemptIncludingRecovery()
    {
        var recoveryConfiguration = new RecoveryConfiguration();
        recoveryConfiguration.Activated(true);
        recoveryConfiguration.Topology(false);
        recoveryConfiguration.BackOffDelayPolicy(new FakeFastBackOffDelay());

        int transportFactoryInvocations = 0;

        ConnectionSettings connectionSettings = _connectionSettingBuilder
            .RecoveryConfiguration(recoveryConfiguration)
            .TransportFactory((host, port, cancellationToken) =>
            {
                Interlocked.Increment(ref transportFactoryInvocations);
                return DialAsync(host, port);
            })
            .Build();

        IConnection connection = await AmqpConnection.CreateAsync(connectionSettings);
        try
        {
            Assert.Equal(State.Open, connection.State);
            Assert.Equal(1, transportFactoryInvocations);

            TaskCompletionSource<bool> recoveredTcs = CreateTaskCompletionSource();
            connection.ChangeState += (sender, previousState, currentState, error) =>
            {
                if (previousState == State.Reconnecting && currentState == State.Open)
                {
                    recoveredTcs.TrySetResult(true);
                }
            };

            await WaitUntilConnectionIsKilled(_containerId);
            await WhenTcsCompletes(recoveredTcs);

            Assert.Equal(State.Open, connection.State);

            // Recovery does not reuse the stream of the connection that died: the factory is asked
            // for exactly one fresh transport for the one recovery attempt that succeeded.
            Assert.Equal(2, transportFactoryInvocations);
        }
        finally
        {
            await connection.CloseAsync();
            connection.Dispose();
        }
    }

    /// <summary>
    /// Opens a plain socket to the host and port the settings named. This is what an application does
    /// in the ordinary case; a proxied application would establish its tunnel here instead.
    /// </summary>
    private static async Task<Stream> DialAsync(string host, int port)
    {
        var tcpClient = new TcpClient();
        await tcpClient.ConnectAsync(host, port);

        // TcpClient.GetStream() hands the socket to the stream, and the stream to the connection,
        // which disposes it when it closes.
        return tcpClient.GetStream();
    }
}

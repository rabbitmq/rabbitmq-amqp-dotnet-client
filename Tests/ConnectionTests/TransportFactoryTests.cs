// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Tests.ConnectionTests;

/// <summary>
/// Tests for <see cref="ConnectionSettings.TransportFactory"/> that need no broker: the stream the
/// factory returns is one end of a loopback socket inside the test process, so these cases run
/// anywhere without a proxy.
/// </summary>
public class TransportFactoryTests
{
    private const string BrokerHost = "broker-a";
    private const int BrokerPort = 5672;

    /// <summary>
    /// Every AMQP connection starts with an eight byte protocol header: the four characters "AMQP",
    /// a protocol id, and the version. Seeing it arrive at the far end of the stream the factory
    /// returned is what proves the library ran the connection over that stream rather than over a
    /// socket it opened itself.
    /// </summary>
    private const string AmqpProtocolMagic = "AMQP";

    private const int AmqpProtocolHeaderLength = 8;
    private const int AmqpMajorVersionOffset = 5;

    private static readonly TimeSpan s_waitSpan = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task ConnectionRunsOnTheStreamTheTransportFactoryReturns()
    {
        int transportFactoryInvocations = 0;
        string? observedHost = null;
        int observedPort = 0;

        using (var loopbackBroker = new LoopbackBroker())
        {
            ConnectionSettings connectionSettings = ConnectionSettingsBuilder.Create()
                .Host(BrokerHost)
                .Port(BrokerPort)
                .ContainerId(nameof(ConnectionRunsOnTheStreamTheTransportFactoryReturns))
                .TransportFactory((host, port, cancellationToken) =>
                {
                    Interlocked.Increment(ref transportFactoryInvocations);
                    observedHost = host;
                    observedPort = port;
                    return loopbackBroker.ConnectAsync();
                })
                .Build();

            Assert.NotNull(connectionSettings.TransportFactory);

            Task<IConnection> connectTask = AmqpConnection.CreateAsync(connectionSettings);

            byte[] protocolHeader = await loopbackBroker
                .ReadAsync(AmqpProtocolHeaderLength)
                .WaitAsync(s_waitSpan);

            Assert.Equal(AmqpProtocolMagic,
                Encoding.ASCII.GetString(protocolHeader, 0, AmqpProtocolMagic.Length));
            Assert.Equal(1, protocolHeader[AmqpMajorVersionOffset]);

            // Nothing on the other end of this socket speaks AMQP, so the open cannot complete. What
            // matters is that the library got as far as writing the header to the supplied stream.
            loopbackBroker.Disconnect();

            await Assert.ThrowsAnyAsync<ConnectionException>(async () => await connectTask);
        }

        // A failed attempt is still one attempt: the factory is not consulted again behind the
        // application's back.
        Assert.Equal(1, transportFactoryInvocations);

        // The factory is handed the broker host and port from the settings, unresolved, so that it
        // can decide for itself how to reach them.
        Assert.Equal(BrokerHost, observedHost);
        Assert.Equal(BrokerPort, observedPort);
    }

    [Theory]
    [InlineData("ws")]
    [InlineData("wss")]
    public async Task WebSocketSchemeWithATransportFactoryIsRejected(string scheme)
    {
        int transportFactoryInvocations = 0;

        ConnectionSettings connectionSettings = ConnectionSettingsBuilder.Create()
            .Scheme(scheme)
            .Host(BrokerHost)
            .Port(BrokerPort)
            .ContainerId(nameof(WebSocketSchemeWithATransportFactoryIsRejected))
            .TransportFactory((host, port, cancellationToken) =>
            {
                Interlocked.Increment(ref transportFactoryInvocations);
                return Task.FromResult<Stream>(new MemoryStream());
            })
            .Build();

        ConnectionException connectionException = await Assert.ThrowsAnyAsync<ConnectionException>(
            async () => await AmqpConnection.CreateAsync(connectionSettings));

        Assert.Contains($"'{scheme}' scheme", connectionException.Message, StringComparison.Ordinal);

        // Rejected rather than silently ignored: no transport is ever asked for.
        Assert.Equal(0, transportFactoryInvocations);
    }

    /// <summary>
    /// A listener on the loopback interface that accepts one connection and only lets the test read
    /// what the client writes to it. It stands in for a broker in the cases that need to observe what
    /// the library puts on the supplied transport, and nothing more.
    /// </summary>
    private sealed class LoopbackBroker : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly Task<TcpClient> _acceptTask;

        private TcpClient? _client;
        private TcpClient? _accepted;

        internal LoopbackBroker()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            _acceptTask = _listener.AcceptTcpClientAsync();
        }

        private int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

        internal async Task<Stream> ConnectAsync()
        {
            _client = new TcpClient();
            await _client.ConnectAsync(IPAddress.Loopback, Port);

            // TcpClient.GetStream() hands the socket to the stream, and the stream to the connection,
            // which disposes it when it closes.
            return _client.GetStream();
        }

        internal async Task<byte[]> ReadAsync(int count)
        {
            _accepted = await _acceptTask;
            NetworkStream stream = _accepted.GetStream();

            byte[] buffer = new byte[count];
            int offset = 0;
            while (offset < count)
            {
                int read = await stream.ReadAsync(buffer, offset, count - offset);
                if (read == 0)
                {
                    throw new EndOfStreamException(
                        $"the transport was closed after {offset} of {count} bytes");
                }

                offset += read;
            }

            return buffer;
        }

        internal void Disconnect()
        {
            _accepted?.Close();
        }

        public void Dispose()
        {
            _accepted?.Dispose();
            _client?.Dispose();
            _listener.Stop();
        }
    }
}

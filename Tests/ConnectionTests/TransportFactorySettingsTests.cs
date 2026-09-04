// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using Xunit;

namespace Tests.ConnectionTests;

/// <summary>
/// How <see cref="ConnectionSettingsBuilder"/> carries a transport factory: onto the settings it
/// builds, and back into a builder made by <see cref="ConnectionSettingsBuilder.From"/>. No
/// connection is opened here, so the factory is never invoked.
/// <para>
///   Both methods branch on the shape of the settings — <see cref="ConnectionSettingsBuilder.Build"/>
///   on whether one URI, several URIs, or a host and port were given, and
///   <see cref="ConnectionSettingsBuilder.From"/> on whether the settings are
///   <see cref="ClusterConnectionSettings"/>, a web-socket scheme, or neither. The four shapes below
///   reach every one of those branches, because a factory dropped on one path only would otherwise
///   look like a working one.
/// </para>
/// </summary>
public class TransportFactorySettingsTests
{
    private const string HostAndPortShape = "host-and-port";
    private const string SingleUriShape = "single-uri";
    private const string WebSocketUriShape = "web-socket-uri";
    private const string ClusterUrisShape = "cluster-uris";

    private const string BrokerHost = "broker-a";
    private const string OtherBrokerHost = "broker-b";
    private const int BrokerPort = 5672;

    [Theory]
    [InlineData(HostAndPortShape)]
    [InlineData(SingleUriShape)]
    [InlineData(WebSocketUriShape)]
    [InlineData(ClusterUrisShape)]
    public void BuilderCarriesTheTransportFactoryOntoTheSettings(string shape)
    {
        ConnectionTransportFactory transportFactory = NotInvokedTransportFactory;

        ConnectionSettings connectionSettings = CreateBuilder(shape)
            .TransportFactory(transportFactory)
            .Build();

        Assert.Same(transportFactory, connectionSettings.TransportFactory);
    }

    [Theory]
    [InlineData(HostAndPortShape)]
    [InlineData(SingleUriShape)]
    [InlineData(WebSocketUriShape)]
    [InlineData(ClusterUrisShape)]
    public void FromCopiesTheTransportFactory(string shape)
    {
        ConnectionTransportFactory transportFactory = NotInvokedTransportFactory;

        ConnectionSettings original = CreateBuilder(shape)
            .TransportFactory(transportFactory)
            .Build();
        Assert.Same(transportFactory, original.TransportFactory);

        ConnectionSettings copy = ConnectionSettingsBuilder.From(original).Build();

        Assert.Same(transportFactory, copy.TransportFactory);
    }

    [Theory]
    [InlineData(HostAndPortShape)]
    [InlineData(SingleUriShape)]
    [InlineData(WebSocketUriShape)]
    [InlineData(ClusterUrisShape)]
    public void FromLeavesTheTransportFactoryUnsetWhenTheSettingsHaveNone(string shape)
    {
        ConnectionSettings original = CreateBuilder(shape).Build();
        Assert.Null(original.TransportFactory);

        ConnectionSettings copy = ConnectionSettingsBuilder.From(original).Build();

        Assert.Null(copy.TransportFactory);
    }

    /// <summary>
    /// The transport factory is how a connection is reached, not which connection it is, so it stays
    /// out of connection identity. Pinned here because <see cref="ConnectionSettings.TransportFactory"/>
    /// says so in its documentation.
    /// </summary>
    [Fact]
    public void TransportFactoryIsNotPartOfConnectionIdentity()
    {
        ConnectionSettings withTransportFactory = CreateBuilder(HostAndPortShape)
            .TransportFactory(NotInvokedTransportFactory)
            .Build();
        ConnectionSettings withoutTransportFactory = CreateBuilder(HostAndPortShape).Build();

        Assert.NotNull(withTransportFactory.TransportFactory);
        Assert.Null(withoutTransportFactory.TransportFactory);

        Assert.Equal(withTransportFactory, withoutTransportFactory);
        Assert.Equal(withTransportFactory.GetHashCode(), withoutTransportFactory.GetHashCode());
    }

    /// <summary>
    /// Builds one of the settings shapes, configured identically apart from the shape itself.
    /// </summary>
    private static ConnectionSettingsBuilder CreateBuilder(string shape)
    {
        ConnectionSettingsBuilder builder = ConnectionSettingsBuilder.Create()
            .ContainerId(nameof(TransportFactorySettingsTests));

        switch (shape)
        {
            case HostAndPortShape:
                return builder
                    .Host(BrokerHost)
                    .Port(BrokerPort);
            case SingleUriShape:
                return builder
                    .Uri(new Uri($"amqp://{BrokerHost}:{BrokerPort}"));
            case WebSocketUriShape:
                return builder
                    .Uri(new Uri($"ws://{BrokerHost}:15678/ws"));
            case ClusterUrisShape:
                return builder
                    .Uris(new[]
                    {
                        new Uri($"amqp://{BrokerHost}:{BrokerPort}"),
                        new Uri($"amqp://{OtherBrokerHost}:{BrokerPort}"),
                    });
            default:
                throw new ArgumentOutOfRangeException(nameof(shape), shape,
                    "unknown connection settings shape");
        }
    }

    /// <summary>
    /// Stands in for a real factory. These cases only ever look at where the delegate ends up, so
    /// being invoked at all is a failure.
    /// </summary>
    private static Task<Stream> NotInvokedTransportFactory(string host, int port,
        CancellationToken cancellationToken)
    {
        throw new InvalidOperationException(
            "the transport factory was invoked, but these tests open no connection");
    }
}

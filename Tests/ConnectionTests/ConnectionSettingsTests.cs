// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Security.Authentication;
using RabbitMQ.AMQP.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests.ConnectionTests;

public class ConnectionSettingsTests(ITestOutputHelper testOutputHelper)
    : IntegrationTest(testOutputHelper, setupConnectionAndManagement: false)
{
    [Fact]
    public void ConnectionSettingsValidation()
    {
        IBackOffDelayPolicy backOffDelayPolicy = new TestBackoffDelayPolicy();
        IRecoveryConfiguration recoveryConfiguration = new TestRecoveryConfiguration(backOffDelayPolicy);

        ConnectionSettings connectionSettings = new("amqp", "localhost", 5672, "guest-user",
            "guest-password", "vhost_1", "connection_name", SaslMechanism.External,
            recoveryConfiguration);
        Assert.Equal("localhost", connectionSettings.Host);
        Assert.Equal(5672, connectionSettings.Port);
        Assert.Equal("guest-user", connectionSettings.User);
        Assert.Equal("guest-password", connectionSettings.Password);
        Assert.Equal("vhost_1", connectionSettings.VirtualHost);
        Assert.Equal("amqp", connectionSettings.Scheme);
        Assert.Equal(SaslMechanism.External, connectionSettings.SaslMechanism);

        ConnectionSettings second = new("amqp", "localhost", 5672, "guest-user",
            "guest-password", "vhost_1", "connection_name", SaslMechanism.External,
            recoveryConfiguration);

        Assert.Equal(connectionSettings, second);

        ConnectionSettings third = new("amqp", "localhost", 5672, "guest-user",
            "guest-password", "path/", "connection_name", SaslMechanism.Plain,
            recoveryConfiguration);

        Assert.NotEqual(connectionSettings, third);

        Assert.Same(recoveryConfiguration, connectionSettings.Recovery);
        Assert.Same(backOffDelayPolicy, connectionSettings.Recovery.GetBackOffDelayPolicy());
    }

    [Fact]
    public void ConnectionSettingsViaBuilder()
    {
        ConnectionSettings connectionSettings = ConnectionSettingsBuilder.Create()
            .Host("localhost")
            .VirtualHost("v1")
            .User("guest-t")
            .Password("guest-w")
            .Scheme("AMQP")
            .Build();

        Assert.Equal("localhost", connectionSettings.Host);
        Assert.Equal(5672, connectionSettings.Port);
        Assert.Equal("guest-t", connectionSettings.User);
        Assert.Equal("guest-w", connectionSettings.Password);
        Assert.Equal("v1", connectionSettings.VirtualHost);
        Assert.Equal("AMQP", connectionSettings.Scheme);
    }

    [Fact]
    public void ConnectionSettingsWithBadSchemeThrows()
    {
        Assert.ThrowsAny<ArgumentOutOfRangeException>(() =>
        {
            ConnectionSettingsBuilder.Create().Scheme("amqpX").Build();
        });

        Assert.ThrowsAny<ArgumentOutOfRangeException>(() =>
        {
            new ConnectionSettings("amqpY", "foobar", 5672);
        });
    }

    [Fact]
    public void ConnectionSettingsViaBuilderWithSslOptions()
    {
        ConnectionSettings connectionSettings = ConnectionSettingsBuilder.Create()
            .Host("localhost")
            .VirtualHost("v1")
            .User("guest-t")
            .Password("guest-w")
            .Scheme("amqps")
            .Build();

        Assert.True(connectionSettings.UseSsl);
        Assert.Equal("localhost", connectionSettings.Host);
        Assert.Equal(5671, connectionSettings.Port);
        Assert.Equal("guest-t", connectionSettings.User);
        Assert.Equal("guest-w", connectionSettings.Password);
        Assert.Equal("v1", connectionSettings.VirtualHost);
        Assert.Equal("amqps", connectionSettings.Scheme);
    }

    [Fact]
    public void ConnectionSettingsViaUri()
    {
        const string scheme = "amqps";
        const string host = "rabbitmq-host.foo.baz.com";
        const string vhost = "/frazzle";
        string user = RandomString(10);
        string pass = RandomString(10);

        var uri = new Uri($"{scheme}://{user}:{pass}@{host}/%2Ffrazzle");
        var connectionSettings = new ConnectionSettings(uri);

        Assert.True(connectionSettings.UseSsl);
        Assert.Equal(host, connectionSettings.Host);
        Assert.Equal(5671, connectionSettings.Port);
        Assert.Equal(user, connectionSettings.User);
        Assert.Equal(pass, connectionSettings.Password);
        Assert.Equal(vhost, connectionSettings.VirtualHost);
        Assert.Equal(scheme, connectionSettings.Scheme);
    }

    [Fact]
    public void ConnectionSettingsViaUris()
    {
        const string scheme = "amqps";
        const string host = "rabbitmq-host.foo.baz.com";
        const string vhost = "/frazzle";
        string user = RandomString(10);
        string pass = RandomString(10);

        var uri0 = new Uri($"{scheme}://{user}:{pass}@{host}:5671/%2Ffrazzle");
        var uri1 = new Uri($"{scheme}://{user}:{pass}@{host}:5681/%2Ffrazzle");
        var uri2 = new Uri($"{scheme}://{user}:{pass}@{host}:5691/%2Ffrazzle");

        List<Uri> uris = [uri0, uri1, uri2];
        var connectionSettings = new ClusterConnectionSettings(uris);

        Assert.True(connectionSettings.UseSsl);
        Assert.Equal(host, connectionSettings.Host);
        Assert.Equal(5671, connectionSettings.Port);
        Assert.Equal(user, connectionSettings.User);
        Assert.Equal(pass, connectionSettings.Password);
        Assert.Equal(vhost, connectionSettings.VirtualHost);
        Assert.Equal(scheme, connectionSettings.Scheme);

        Assert.NotNull(connectionSettings.Addresses);
        Amqp.Address a0 = connectionSettings.Addresses[0];
        Assert.Equal(host, a0.Host);
        Assert.Equal(5671, a0.Port);
        Assert.Equal(user, a0.User);
        Assert.Equal(pass, a0.Password);
        Assert.Equal(scheme, a0.Scheme);

        Amqp.Address a1 = connectionSettings.Addresses[1];
        Assert.Equal(host, a1.Host);
        Assert.Equal(5681, a1.Port);
        Assert.Equal(user, a1.User);
        Assert.Equal(pass, a1.Password);
        Assert.Equal(scheme, a1.Scheme);

        Amqp.Address a2 = connectionSettings.Addresses[2];
        Assert.Equal(host, a2.Host);
        Assert.Equal(5691, a2.Port);
        Assert.Equal(user, a2.User);
        Assert.Equal(pass, a2.Password);
        Assert.Equal(scheme, a2.Scheme);
    }

    [Fact]
    public void ConnectionSettingsViaBuilderWithUris()
    {
        const string scheme = "amqps";
        const string host = "rabbitmq-host.foo.baz.com";
        const string vhost = "/frazzle";
        const uint maxFrameSize = 1234;
        string user = RandomString(10);
        string pass = RandomString(10);
        string containerId = RandomString(10);

        var uri0 = new Uri($"{scheme}://{user}:{pass}@{host}:5671/%2Ffrazzle");
        var uri1 = new Uri($"{scheme}://{user}:{pass}@{host}:5681/%2Ffrazzle");
        var uri2 = new Uri($"{scheme}://{user}:{pass}@{host}:5691/%2Ffrazzle");

        IBackOffDelayPolicy backOffDelayPolicy = new TestBackoffDelayPolicy();
        IRecoveryConfiguration recoveryConfiguration = new TestRecoveryConfiguration(backOffDelayPolicy);
        TlsSettings tlsSettings = new(SslProtocols.Tls12);

        List<Uri> uris = [uri0, uri1, uri2];
        ConnectionSettingsBuilder connectionSettingsBuilder = ConnectionSettingsBuilder.Create()
            .Uris(uris)
            .ContainerId(containerId)
            .SaslMechanism(SaslMechanism.Anonymous)
            .RecoveryConfiguration(recoveryConfiguration)
            .MaxFrameSize(maxFrameSize)
            .TlsSettings(tlsSettings);
        ConnectionSettings connectionSettings = connectionSettingsBuilder.Build();

        Assert.Same(recoveryConfiguration, connectionSettings.Recovery);
        Assert.Same(backOffDelayPolicy, connectionSettings.Recovery.GetBackOffDelayPolicy());
        Assert.Same(tlsSettings, connectionSettings.TlsSettings);
        Assert.True(connectionSettings.UseSsl);
        Assert.Equal(SaslMechanism.Anonymous, connectionSettings.SaslMechanism);
        Assert.Equal(containerId, connectionSettings.ContainerId);
        Assert.Equal(maxFrameSize, connectionSettings.MaxFrameSize);
        Assert.Equal(host, connectionSettings.Host);
        Assert.Equal(5671, connectionSettings.Port);
        Assert.Equal(user, connectionSettings.User);
        Assert.Equal(pass, connectionSettings.Password);
        Assert.Equal(vhost, connectionSettings.VirtualHost);
        Assert.Equal(scheme, connectionSettings.Scheme);

        Assert.NotNull(connectionSettings.Addresses);
        Amqp.Address a0 = connectionSettings.Addresses[0];
        Assert.Equal(host, a0.Host);
        Assert.Equal(5671, a0.Port);
        Assert.Equal(user, a0.User);
        Assert.Equal(pass, a0.Password);
        Assert.Equal(scheme, a0.Scheme);

        Amqp.Address a1 = connectionSettings.Addresses[1];
        Assert.Equal(host, a1.Host);
        Assert.Equal(5681, a1.Port);
        Assert.Equal(user, a1.User);
        Assert.Equal(pass, a1.Password);
        Assert.Equal(scheme, a1.Scheme);

        Amqp.Address a2 = connectionSettings.Addresses[2];
        Assert.Equal(host, a2.Host);
        Assert.Equal(5691, a2.Port);
        Assert.Equal(user, a2.User);
        Assert.Equal(pass, a2.Password);
        Assert.Equal(scheme, a2.Scheme);
    }

    [Fact]
    public void ConnectionSettingsWithEqualUris()
    {
        const string scheme = "amqps";
        const string host = "rabbitmq-host.foo.baz.com";
        string user = RandomString(10);
        string pass = RandomString(10);
        string containerId = RandomString(10);

        var uri0_0 = new Uri($"{scheme}://{user}:{pass}@{host}:5681/%2Ffrazzle");
        var uri0_1 = new Uri($"{scheme}://{user}:{pass}@{host}:5681/%2Ffrazzle");

        var uri1_0 = new Uri($"{scheme}://{user}:{pass}@{host}:5691/%2Ffrazzle");
        var uri1_1 = new Uri($"{scheme}://{user}:{pass}@{host}:5691/%2Ffrazzle");

        var uri2_0 = new Uri($"{scheme}://{user}:{pass}@{host}:5677/%2Ffrazzle");
        var uri2_1 = new Uri($"{scheme}://{user}:{pass}@{host}:5677/%2Ffrazzle");

        List<Uri> uris0 = [uri0_0, uri1_0, uri2_0];
        ConnectionSettingsBuilder connectionSettingsBuilder0 = ConnectionSettingsBuilder.Create()
            .Uris(uris0)
            .ContainerId(containerId);
        ConnectionSettings connectionSettings0 = connectionSettingsBuilder0.Build();

        List<Uri> uris1 = [uri0_1, uri1_1, uri2_1];
        ConnectionSettingsBuilder connectionSettingsBuilder1 = ConnectionSettingsBuilder.Create()
            .Uris(uris1)
            .ContainerId(containerId);
        ConnectionSettings connectionSettings1 = connectionSettingsBuilder1.Build();
        Assert.Equal(connectionSettings0, connectionSettings1);
        Assert.Equal(connectionSettings0.GetHashCode(), connectionSettings1.GetHashCode());
    }

    [Fact]
    public void ConnectionSettingsWithUrisNotEqual()
    {
        const string scheme = "amqps";
        const string host = "rabbitmq-host.foo.baz.com";
        string user = RandomString(10);
        string pass = RandomString(10);
        string containerId = RandomString(10);

        var uri0 = new Uri($"{scheme}://{user}:{pass}@{host}:5681/%2Ffrazzle");
        var uri1 = new Uri($"{scheme}://{user}:{pass}@{host}:5691/%2Ffrazzle");
        var uri2_0 = new Uri($"{scheme}://{user}:{pass}@{host}:5671/%2Ffrazzle");
        var uri2_1 = new Uri($"{scheme}://{user}:{pass}@{host}:5677/%2Ffrazzle");

        List<Uri> uris0 = [uri0, uri1, uri2_0];
        ConnectionSettingsBuilder connectionSettingsBuilder0 = ConnectionSettingsBuilder.Create()
            .Uris(uris0)
            .ContainerId(containerId);
        ConnectionSettings connectionSettings0 = connectionSettingsBuilder0.Build();

        List<Uri> uris1 = [uri0, uri1, uri2_1];
        ConnectionSettingsBuilder connectionSettingsBuilder1 = ConnectionSettingsBuilder.Create()
            .Uris(uris1)
            .ContainerId(containerId);
        ConnectionSettings connectionSettings1 = connectionSettingsBuilder1.Build();
        Assert.NotEqual(connectionSettings0, connectionSettings1);
        Assert.NotEqual(connectionSettings0.GetHashCode(), connectionSettings1.GetHashCode());
    }

    [Fact]
    public void ConnectionSettingsViaUrisThrowsWithDifferentVirtualHosts()
    {
        const string scheme = "amqps";
        const string host = "rabbitmq-host.foo.baz.com";
        string user = RandomString(10);
        string pass = RandomString(10);

        var uri0 = new Uri($"{scheme}://{user}:{pass}@{host}:5671/foo");
        var uri1 = new Uri($"{scheme}://{user}:{pass}@{host}:5681/bar");
        var uri2 = new Uri($"{scheme}://{user}:{pass}@{host}:5691/foo");

        List<Uri> uris = [uri0, uri1, uri2];
        Assert.ThrowsAny<ArgumentException>(() => new ClusterConnectionSettings(uris));
    }

    [Fact]
    public void BuilderThrowsWhenUriAndUrisBothSet()
    {
        const string scheme = "amqps";
        const string host = "rabbitmq-host.foo.baz.com";
        string user = RandomString(10);
        string pass = RandomString(10);
        string containerId = RandomString(10);

        var uri0 = new Uri($"{scheme}://{user}:{pass}@{host}:5671/%2Ffrazzle");
        var uri1 = new Uri($"{scheme}://{user}:{pass}@{host}:5681/%2Ffrazzle");
        var uri2 = new Uri($"{scheme}://{user}:{pass}@{host}:5691/%2Ffrazzle");

        List<Uri> uris = [uri0, uri1, uri2];
        Assert.ThrowsAny<ArgumentOutOfRangeException>(() =>
        {
            ConnectionSettingsBuilder.Create().Uri(uri0).Uris(uris);
        });
    }

    private class TestBackoffDelayPolicy : IBackOffDelayPolicy
    {
        public int CurrentAttempt => 1;
        public int Delay() => 1;
        public bool IsActive() => true;
        public void Reset() { }
    }

    private class TestRecoveryConfiguration : IRecoveryConfiguration
    {
        private readonly IBackOffDelayPolicy _backOffDelayPolicy;

        public TestRecoveryConfiguration(IBackOffDelayPolicy backOffDelayPolicy)
        {
            _backOffDelayPolicy = backOffDelayPolicy;
        }

        public IRecoveryConfiguration Activated(bool activated) => this;
        public IRecoveryConfiguration BackOffDelayPolicy(IBackOffDelayPolicy backOffDelayPolicy) => this;
        public IBackOffDelayPolicy GetBackOffDelayPolicy() => _backOffDelayPolicy;
        public bool IsActivated() => true;
        public bool IsTopologyActive() => true;
        public IRecoveryConfiguration Topology(bool activated) => this;
    }
}

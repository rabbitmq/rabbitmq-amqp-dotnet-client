// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.IO;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class TlsConnectionTests : IntegrationTest
{
    private static readonly ushort[] s_ports = [5671, 5681, 5691];

    private readonly ITestOutputHelper _output;
    private readonly HttpApiClient _httpApiClient = new();
    private readonly bool _isRunningInCI = IsRunningInCI;

    private ushort _port = 5671;

    public TlsConnectionTests(ITestOutputHelper output) : base(output,
        setupConnectionAndManagement: false)
    {
        _output = output;
    }

    public override Task InitializeAsync()
    {
        if (IsCluster)
        {
            _port = s_ports[Utils.RandomNext(0, s_ports.Length)];
        }

        return base.InitializeAsync();
    }

    public override Task DisposeAsync()
    {
        _httpApiClient.Dispose();
        return base.DisposeAsync();
    }

    [Fact]
    public async Task ConnectUsingTlsAndUserPassword()
    {
        ConnectionSettings connectionSettings = _connectionSettingBuilder
            .Scheme("amqps")
            .Port(_port)
            .Build();
        Assert.True(connectionSettings.UseSsl);
        Assert.NotNull(connectionSettings.TlsSettings);

#if NETFRAMEWORK
        connectionSettings.TlsSettings.Protocols = SslProtocols.Tls12;
#endif

        if (_isRunningInCI)
        {
            /*
             * Note: when running on GitHub actions, the CA cert has been installed in the system store,
             * so client applications (like these tests) can validate the server cert. This step isn't taken
             * in local environments, so ignore cert chain errors in that case
             */
        }
        else
        {
            connectionSettings.TlsSettings.AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors;
            connectionSettings.TlsSettings.RemoteCertificateValidationCallback = (sender, certificate, chain, errors) => true;
        }

        Assert.Equal("localhost", connectionSettings.Host);
        Assert.Equal(_port, connectionSettings.Port);
        Assert.Equal("guest", connectionSettings.User);
        Assert.Equal("guest", connectionSettings.Password);
        Assert.Equal("/", connectionSettings.VirtualHost);
        Assert.Equal("amqps", connectionSettings.Scheme);

        IConnection connection = await AmqpConnection.CreateAsync(connectionSettings);
        Assert.Equal(State.Open, connection.State);
        await connection.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
    }

    [Fact]
    public async Task ConnectUsingTlsAndClientCertificate()
    {
        string clientCertFile = GetClientCertFile();
        var cert = new X509Certificate2(clientCertFile, "grapefruit");

        await CreateUserFromCertSubject(cert);

        ConnectionSettings connectionSettings = _connectionSettingBuilder
            .Scheme("amqps")
            .SaslMechanism(SaslMechanism.External)
            .Port(_port)
            .Build();

        Assert.True(connectionSettings.UseSsl);
        Assert.NotNull(connectionSettings.TlsSettings);

        connectionSettings.TlsSettings.ClientCertificates.Add(cert);
#if NETFRAMEWORK
        connectionSettings.TlsSettings.Protocols = SslProtocols.Tls12;
#endif

        if (_isRunningInCI)
        {
            /*
             * Note: when running on GitHub actions, the CA cert has been installed in the system store,
             * so client applications (like these tests) can validate the server cert. This step isn't taken
             * in local environments, so ignore cert chain errors in that case
             */
        }
        else
        {
            connectionSettings.TlsSettings.AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors;
            connectionSettings.TlsSettings.RemoteCertificateValidationCallback = (sender, certificate, chain, errors) => true;
        }

        Assert.Equal("localhost", connectionSettings.Host);
        Assert.Equal(_port, connectionSettings.Port);
        Assert.Null(connectionSettings.User);
        Assert.Null(connectionSettings.Password);
        Assert.Equal("/", connectionSettings.VirtualHost);
        Assert.Equal("amqps", connectionSettings.Scheme);
        Assert.Equal(SaslMechanism.External, connectionSettings.SaslMechanism);

        IConnection connection = await AmqpConnection.CreateAsync(connectionSettings);
        Assert.Equal(State.Open, connection.State);
        await connection.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
    }

    private Task CreateUserFromCertSubject(X509Certificate cert)
    {
        string userName = cert.Subject.Trim().Replace(" ", string.Empty);
        return _httpApiClient.CreateUserAsync(userName);
    }

    private static string GetClientCertFile()
    {
        string cwd = Directory.GetCurrentDirectory();
        string clientCertFile = Path.GetFullPath(Path.Combine(cwd, "../../../../.ci/certs/client_localhost.p12"));
        if (false == File.Exists(clientCertFile))
        {
            clientCertFile = Path.GetFullPath(Path.Combine(cwd, "../../../../../.ci/certs/client_localhost.p12"));
        }
        Assert.True(File.Exists(clientCertFile));
        return clientCertFile;
    }
}

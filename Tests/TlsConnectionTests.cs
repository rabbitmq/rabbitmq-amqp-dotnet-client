// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.IO;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using EasyNetQ.Management.Client;
using EasyNetQ.Management.Client.Model;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class TlsConnectionTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private readonly Uri _managementUri = new("http://localhost:15672");
    private readonly ManagementClient _managementClient;
    private readonly bool _isRunningInCI = SystemUtils.IsRunningInCI;

    public TlsConnectionTests(ITestOutputHelper output)
    {
        _output = output;
        _managementClient = new ManagementClient(_managementUri, "guest", "guest");
    }

    public Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        _managementClient.Dispose();
        return Task.CompletedTask;
    }

    [Fact]
    public async Task ConnectUsingTlsAndUserPassword()
    {
        ConnectionSettings connectionSettings = ConnectionSettingBuilder.Create()
            .Host("localhost")
            .Scheme("amqps")
            .Build();

        Assert.True(connectionSettings.UseSsl);
        Assert.NotNull(connectionSettings.TlsSettings);

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
        }

        Assert.Equal("localhost", connectionSettings.Host);
        Assert.Equal(5671, connectionSettings.Port);
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

        ConnectionSettings connectionSettings = ConnectionSettingBuilder.Create()
            .Host("localhost")
            .Scheme("amqps")
            .SaslMechanism(SaslMechanism.External)
            .Build();

        Assert.True(connectionSettings.UseSsl);
        Assert.NotNull(connectionSettings.TlsSettings);
        connectionSettings.TlsSettings.ClientCertificates.Add(cert);

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
        }

        Assert.Equal("localhost", connectionSettings.Host);
        Assert.Equal(5671, connectionSettings.Port);
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

    private async Task CreateUserFromCertSubject(X509Certificate cert)
    {
        string userName = cert.Subject.Trim().Replace(" ", string.Empty);

        var userInfo = new UserInfo(null, null, []);
        await _managementClient.CreateUserAsync(userName, userInfo);

        var permissionInfo = new PermissionInfo();
        await _managementClient.CreatePermissionAsync("/", userName, permissionInfo);
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

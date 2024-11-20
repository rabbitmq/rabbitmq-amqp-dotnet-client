﻿// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.ConnectionTests;

public class SaslConnectionTests(ITestOutputHelper testOutputHelper)
    : IntegrationTest(testOutputHelper, setupConnectionAndManagement: false)
{
    [Fact]
    public async Task ConnectUsingSaslAnonymous()
    {
        Assert.Null(_connection);
        Assert.Null(_management);

        ConnectionSettingBuilder connectionSettingBuilder = ConnectionSettingBuilder.Create();

        _containerId = $"{_testDisplayName}:{Now}";
        connectionSettingBuilder.ContainerId(_containerId);
        connectionSettingBuilder.SaslMechanism(SaslMechanism.Anonymous);

        ConnectionSettings connectionSettings = connectionSettingBuilder.Build();
        _connection = await AmqpConnection.CreateAsync(connectionSettings);

        Assert.Equal(State.Open, _connection.State);

        await _connection.CloseAsync();
        Assert.Equal(State.Closed, _connection.State);
    }

    [Fact]
    public async Task ConnectUsingSaslPlain()
    {
        Assert.Null(_connection);
        Assert.Null(_management);

        ConnectionSettingBuilder connectionSettingBuilder = ConnectionSettingBuilder.Create();

        _containerId = $"{_testDisplayName}:{Now}";
        connectionSettingBuilder.ContainerId(_containerId);
        connectionSettingBuilder.SaslMechanism(SaslMechanism.Plain);

        ConnectionSettings connectionSettings = connectionSettingBuilder.Build();
        _connection = await AmqpConnection.CreateAsync(connectionSettings);

        Assert.Equal(State.Open, _connection.State);

        await _connection.CloseAsync();
        Assert.Equal(State.Closed, _connection.State);
    }
}

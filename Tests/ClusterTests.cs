// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class ClusterTests(ITestOutputHelper testOutputHelper)
    : IntegrationTest(testOutputHelper, setupConnectionAndManagement: false)
{
    [SkippableFact]
    public Task CreateConnectionWithEnvironmentAndMultipleUris()
    {
        Skip.IfNot(IsCluster);

        Assert.Null(_connection);
        Assert.Null(_management);

        Uri uri0 = new("amqp://localhost:5672");
        Uri uri1 = new("amqp://localhost:5673");
        Uri uri2 = new("amqp://localhost:5674");
        List<Uri> uris = [uri0, uri1, uri2];

        ConnectionSettingsBuilder connectionSettingBuilder = new();
        connectionSettingBuilder.Uris(uris);
        ConnectionSettings connectionSettings = connectionSettingBuilder.Build();

        /*
        IEnvironment env = AmqpEnvironment.Create(ConnectionSettingBuilder.Create().Build());
        IConnection connection = await env.CreateConnectionAsync();
        Assert.NotNull(connection);
        Assert.NotEmpty(env.GetConnections());
        await env.CloseAsync();
        Assert.Equal(State.Closed, connection.State);
        Assert.Empty(env.GetConnections());
        */

        return Task.CompletedTask;
    }
}

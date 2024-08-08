// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public abstract class IntegrationTest : IAsyncLifetime
{
    protected readonly ITestOutputHelper _testOutputHelper;
    protected readonly string _testDisplayName = nameof(AmqpTests);
    protected readonly TimeSpan _waitSpan = TimeSpan.FromSeconds(5);

    protected IConnection? _connection;
    protected string _connectionName = $"integration-test-{Now}";

    public IntegrationTest(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;

        Type testOutputHelperType = _testOutputHelper.GetType();
        FieldInfo? testMember = testOutputHelperType.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
        if (testMember is not null)
        {
            object? testObj = testMember.GetValue(_testOutputHelper);
            if (testObj is ITest test)
            {
                _testDisplayName = test.DisplayName;
            }
        }
    }

    public virtual async Task InitializeAsync()
    {
        ConnectionSettingBuilder connectionSettingBuilder = ConnectionSettingBuilder.Create();

        _connectionName = $"{_testDisplayName}:{Now}";
        connectionSettingBuilder.ConnectionName(_connectionName);

        ConnectionSettings connectionSettings = connectionSettingBuilder.Build();
        _connection = await AmqpConnection.CreateAsync(connectionSettings);
    }

    public virtual async Task DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.CloseAsync();
            _connection.Dispose();
        }
    }

    protected static string Now => DateTime.UtcNow.ToString("s", CultureInfo.InvariantCulture);

    protected Task WhenAllComplete(IEnumerable<Task> tasks)
    {
        return Task.WhenAll(tasks).WaitAsync(_waitSpan);
    }

    protected Task WhenTaskCompletes(Task task)
    {
        return task.WaitAsync(_waitSpan);
    }
}

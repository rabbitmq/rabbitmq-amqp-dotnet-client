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
    protected IManagement? _management;
    protected string _queueName;
    protected string _exchangeName;
    protected string _containerId = $"integration-test-{Now}";

    private readonly bool _setupConnectionAndManagement;

    public IntegrationTest(ITestOutputHelper testOutputHelper,
        bool setupConnectionAndManagement = true)
    {
        _testOutputHelper = testOutputHelper;
        _setupConnectionAndManagement = setupConnectionAndManagement;
        _queueName = $"{_testDisplayName}-queue-{Now}";
        _exchangeName = $"{_testDisplayName}-exchange-{Now}";

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
        if (_setupConnectionAndManagement)
        {
            ConnectionSettingBuilder connectionSettingBuilder = ConnectionSettingBuilder.Create();

            _containerId = $"{_testDisplayName}:{Now}";
            connectionSettingBuilder.ContainerId(_containerId);

            ConnectionSettings connectionSettings = connectionSettingBuilder.Build();
            _connection = await AmqpConnection.CreateAsync(connectionSettings);
            _management = _connection.Management();
        }

        /*
         * Note: re-assigning here since Theory tests
         * will include the theory data at this point
         */
        _queueName = $"{_testDisplayName}-queue-{Now}";
        _exchangeName = $"{_testDisplayName}-exchange-{Now}";
    }

    public virtual async Task DisposeAsync()
    {
        if (_management is not null)
        {
            if (_management.State == State.Open)
            {
                try
                {
                    IQueueSpecification queueSpecification = _management.Queue(_queueName);
                    await queueSpecification.DeleteAsync();
                }
                catch
                {
                }

                try
                {
                    IExchangeSpecification exchangeSpecification = _management.Exchange(_exchangeName);
                    await exchangeSpecification.DeleteAsync();
                }
                catch
                {
                }

                await _management.CloseAsync();
            }
            Assert.Equal(State.Closed, _management.State);
            _management.Dispose();
        }

        if (_connection is not null && _connection.State == State.Open)
        {
            if (_connection.State == State.Open)
            {
                await _connection.CloseAsync();
            }
            Assert.Equal(State.Closed, _connection.State);
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

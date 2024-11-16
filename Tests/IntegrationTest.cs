// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public abstract class IntegrationTest : IAsyncLifetime
{
#if NET6_0_OR_GREATER
    private static readonly Random s_random = Random.Shared;
#else
    [ThreadStatic] private static Random? s_random;
#endif

    protected readonly ITestOutputHelper _testOutputHelper;
    protected readonly string _testDisplayName = nameof(IntegrationTest);
    protected readonly TimeSpan _waitSpan = TimeSpan.FromSeconds(5);

    protected IConnection? _connection;
    protected ConnectionSettings? _connectionSettings;
    protected IManagement? _management;
    protected string _queueName;
    protected string _exchangeName;
    protected string _containerId = $"integration-test-{Now}";

    private readonly bool _setupConnectionAndManagement;
    protected readonly ConnectionSettingBuilder _connectionSettingBuilder;

    protected bool _areFilterExpressionsSupported = false;

    public IntegrationTest(ITestOutputHelper testOutputHelper,
        bool setupConnectionAndManagement = true)
    {
        _testOutputHelper = testOutputHelper;
        _setupConnectionAndManagement = setupConnectionAndManagement;

        _testDisplayName = InitTestDisplayName();

        _queueName = $"{_testDisplayName}-queue-{Now}";
        _exchangeName = $"{_testDisplayName}-exchange-{Now}";
        _containerId = $"{_testDisplayName}:{Now}";

        if (SystemUtils.IsVerbose)
        {
            _testOutputHelper.WriteLine("{0} [DEBUG] [START] {1}", DateTime.Now, _testDisplayName);
        }

        _connectionSettingBuilder = InitConnectionSettingsBuilder();
    }

    public virtual async Task InitializeAsync()
    {
        if (_setupConnectionAndManagement)
        {
            _connectionSettings = _connectionSettingBuilder.Build();
            _connection = await AmqpConnection.CreateAsync(_connectionSettings);
            if (_connection is AmqpConnection amqpConnection)
            {
                _areFilterExpressionsSupported = amqpConnection.AreFilterExpressionsSupported;
            }
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
        if (SystemUtils.IsVerbose)
        {
            _testOutputHelper.WriteLine("{0} [DEBUG] [START DISPOSE] {1}", DateTime.Now, _testDisplayName);
        }

        if (_management is not null && _management.State == State.Open)
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
            Assert.Equal(State.Closed, _management.State);
            _management.Dispose();
        }

        if (_connection is not null && _connection.State == State.Open)
        {
            await _connection.CloseAsync();
            Assert.Equal(State.Closed, _connection.State);
            _connection.Dispose();
        }

        if (SystemUtils.IsVerbose)
        {
            _testOutputHelper.WriteLine("{0} [DEBUG] [END DISPOSE] {1}", DateTime.Now, _testDisplayName);
        }
    }

    protected static Random S_Random
    {
        get
        {
#if NET6_0_OR_GREATER
            return s_random;
#else
            s_random ??= new Random();
            return s_random;
#endif
        }
    }

    protected static string Now => DateTime.UtcNow.ToString("s", CultureInfo.InvariantCulture);

    protected static TaskCompletionSource<bool> CreateTaskCompletionSource()
    {
        return CreateTaskCompletionSource<bool>();
    }

    protected static TaskCompletionSource<T> CreateTaskCompletionSource<T>()
    {
        return new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    protected Task<T> WhenTcsCompletes<T>(TaskCompletionSource<T> tcs)
    {
        return tcs.Task.WaitAsync(_waitSpan);
    }

    protected Task WhenAllComplete(params Task[] tasks)
    {
        return Task.WhenAll(tasks).WaitAsync(_waitSpan);
    }

    protected Task WhenAllComplete(IEnumerable<Task> tasks)
    {
        return Task.WhenAll(tasks).WaitAsync(_waitSpan);
    }

    protected Task WhenTaskCompletes(Task task)
    {
        return task.WaitAsync(_waitSpan);
    }

    protected async Task WaitUntilStable(Func<int> getValue)
    {
        const int iterations = 20;
        int iteration = 0;
        int val0 = int.MinValue;
        int val1 = int.MaxValue;
        do
        {
            await Task.Delay(250);
            val0 = getValue();
            await Task.Delay(250);
            val1 = getValue();
            iteration++;
        } while (val0 != val1 && iteration < iterations);

        if (iteration == iterations)
        {
            Assert.Fail("value did not stabilize as expected");
        }
    }

    protected async Task PublishAsync(IQueueSpecification queueSpecification, ulong messageCount,
        Action<ulong, IMessage>? messageLogic = null)
    {
        Assert.NotNull(_connection);

        IPublisherBuilder publisherBuilder = _connection.PublisherBuilder().Queue(queueSpecification);
        IPublisher publisher = await publisherBuilder.BuildAsync();
        try
        {
            var publishTasks = new List<Task<PublishResult>>();
            for (ulong i = 0; i < messageCount; i++)
            {
                var message = new AmqpMessage($"message_{i}");
                if (messageLogic is null)
                {
                    message.MessageId(i.ToString());
                }
                else
                {
                    messageLogic(i, message);
                }

                publishTasks.Add(publisher.PublishAsync(message));
            }

            await WhenAllComplete(publishTasks);

            foreach (Task<PublishResult> pt in publishTasks)
            {
                PublishResult pr = await pt;
                Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);
            }
        }
        finally
        {
            await publisher.CloseAsync();
            publisher.Dispose();
        }
    }

    protected async Task<IEnumerable<IMessage>> ConsumeAsync(ulong expectedMessageCount,
        Action<IConsumerBuilder.IStreamFilterOptions> streamFilterOptionsLogic)
    {
        Assert.NotNull(_connection);

        TaskCompletionSource<bool> allMessagesConsumedTcs = CreateTaskCompletionSource();
        int receivedMessageCount = 0;

        var messages = new List<IMessage>();
        SemaphoreSlim messagesSemaphore = new(1, 1);
        async Task MessageHandler(IContext cxt, IMessage msg)
        {
            await messagesSemaphore.WaitAsync();
            try
            {
                messages.Add(msg);
                receivedMessageCount++;
                if (receivedMessageCount == (int)expectedMessageCount)
                {
                    allMessagesConsumedTcs.SetResult(true);
                }
                cxt.Accept();
            }
            catch (Exception ex)
            {
                allMessagesConsumedTcs.SetException(ex);
            }
            finally
            {
                messagesSemaphore.Release();
            }
        }

        IConsumerBuilder consumerBuilder = _connection.ConsumerBuilder().Queue(_queueName).MessageHandler(MessageHandler);
        streamFilterOptionsLogic(consumerBuilder.Stream().Offset(StreamOffsetSpecification.First).Filter());

        using (IConsumer consumer = await consumerBuilder.BuildAndStartAsync())
        {
            await WhenTcsCompletes(allMessagesConsumedTcs);
            await consumer.CloseAsync();
        }

        return messages;
    }

    protected static byte[] RandomBytes(uint length = 128)
    {
        byte[] buffer = new byte[length];
        S_Random.NextBytes(buffer);
        return buffer;
    }

    protected static int RandomNext(int minValue = 0, int maxValue = 1024)
    {
        return S_Random.Next(minValue, maxValue);
    }

    protected static string RandomString(uint length = 128)
    {
        const string str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
        int strLen = str.Length;

        StringBuilder sb = new((int)length);

        int idx;
        for (int i = 0; i < length; i++)
        {
            idx = RandomNext(0, strLen);
            sb.Append(str[idx]);
        }
        return sb.ToString();
    }

    private string InitTestDisplayName()
    {
        string rv = _testDisplayName;

        Type testOutputHelperType = _testOutputHelper.GetType();
        FieldInfo? testMember = testOutputHelperType.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
        if (testMember is not null)
        {
            object? testObj = testMember.GetValue(_testOutputHelper);
            if (testObj is ITest test)
            {
                rv = test.DisplayName;
            }
        }

        return rv;
    }

    private ConnectionSettingBuilder InitConnectionSettingsBuilder()
    {
        if (string.IsNullOrWhiteSpace(_containerId))
        {
            // TODO create "internal bug" exception type?
            throw new InvalidOperationException("_containerId is null or whitespace," +
                                                " report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
        }

        var connectionSettingBuilder = ConnectionSettingBuilder.Create();
        connectionSettingBuilder.ContainerId(_containerId);
        connectionSettingBuilder.Host(SystemUtils.RabbitMqHost);

        if (SystemUtils.IsCluster)
        {
            // Note: 5672,5673 and 5674 could be returned here
            connectionSettingBuilder.Port(Utils.RandomNext(5672, 5675));
        }

        return connectionSettingBuilder;
    }
}

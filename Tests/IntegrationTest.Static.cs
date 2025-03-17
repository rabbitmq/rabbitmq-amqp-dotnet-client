// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using Xunit.Sdk;

namespace Tests;

public abstract partial class IntegrationTest
{
    const string DefaultRabbitMqHost = "localhost";
    private static readonly Regex s_testDisplayNameRegex = new Regex("^Tests\\.", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly HttpApiClient s_httpApiClient = new();
    private static readonly string s_rabbitMqHost = InitRabbitMqHost();
    private static readonly bool s_isRunningInCI = InitIsRunningInCI();
    private static readonly bool s_isVerbose = InitIsVerbose();
    private static readonly bool s_isCluster;
    private static readonly ushort s_clusterSize;
    private static readonly TimeSpan s_initialDelaySpan = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan s_shortDelaySpan = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan s_delaySpan = TimeSpan.FromMilliseconds(500);

    protected static string RabbitMqHost => s_rabbitMqHost;
    protected static bool IsRunningInCI => s_isRunningInCI;
    protected static bool IsVerbose => s_isVerbose;
    protected static bool IsCluster => s_isCluster;
    protected static ushort ClusterSize => s_clusterSize;

    static IntegrationTest()
    {
        s_clusterSize = s_httpApiClient.GetClusterSize();
        s_isCluster = s_clusterSize > 1;
    }

    protected static async Task WaitUntilFuncAsync(Func<bool> func, ushort retries = 20)
    {
        int tries = 0;
        DateTime start = DateTime.Now;

        if (IsRunningInCI)
        {
            retries *= 2;
        }

        await Task.Delay(s_initialDelaySpan);

        while (false == func())
        {
            tries++;
            await Task.Delay(s_shortDelaySpan);

            --retries;
            if (retries == 0)
            {
                DateTime end = DateTime.Now;
                TimeSpan duration = end - start;
                throw new XunitException($"timed out waiting on a condition after {duration}, tries: {tries}");
            }
        }
    }

    protected static async Task WaitUntilFuncAsync(Func<Task<bool>> func, ushort retries = 20)
    {
        int tries = 0;
        DateTime start = DateTime.Now;

        if (IsRunningInCI)
        {
            retries *= 2;
        }

        await Task.Delay(s_initialDelaySpan);

        while (false == await func())
        {
            tries++;
            await Task.Delay(s_delaySpan);

            --retries;
            if (retries == 0)
            {
                DateTime end = DateTime.Now;
                TimeSpan duration = end - start;
                throw new XunitException($"timed out waiting on a condition after {duration}, tries: {tries}");
            }
        }
    }

    protected static Task WaitUntilConnectionIsOpen(string containerId)
    {
        return WaitUntilFuncAsync(() => s_httpApiClient.CheckConnectionAsync(containerId, checkOpened: true));
    }

    protected static Task WaitUntilConnectionIsClosed(string containerId)
    {
        return WaitUntilFuncAsync(() => s_httpApiClient.CheckConnectionAsync(containerId, checkOpened: false));
    }

    protected static async Task WaitUntilConnectionIsKilled(string containerId)
    {
        await WaitUntilConnectionIsOpen(containerId);
        await WaitUntilFuncAsync(async () =>
            await s_httpApiClient.KillConnectionAsync(containerId) == 1);
    }

    protected static async Task WaitUntilConnectionIsKilledAndOpen(string containerId)
    {
        await WaitUntilConnectionIsOpen(containerId);
        await WaitUntilFuncAsync(async () => await s_httpApiClient.KillConnectionAsync(containerId) == 1);
        await WaitUntilConnectionIsOpen(containerId);
    }

    protected static Task WaitUntilQueueExistsAsync(IQueueSpecification queueSpec)
    {
        return WaitUntilQueueExistsAsync(queueSpec.QueueName);
    }

    protected static Task WaitUntilQueueExistsAsync(string queueNameStr)
    {
        return WaitUntilFuncAsync(() => { return s_httpApiClient.CheckQueueAsync(queueNameStr, checkExisting: true); });
    }

    protected static Task WaitUntilQueueDeletedAsync(IQueueSpecification queueSpec)
    {
        return WaitUntilQueueDeletedAsync(queueSpec.QueueName);
    }

    protected static Task WaitUntilQueueDeletedAsync(string queueNameStr)
    {
        return WaitUntilFuncAsync(() => { return s_httpApiClient.CheckQueueAsync(queueNameStr, checkExisting: false); });
    }

    protected static Task WaitUntilExchangeExistsAsync(IExchangeSpecification exchangeSpec)
    {
        return WaitUntilExchangeExistsAsync(exchangeSpec.ExchangeName);
    }

    protected static Task WaitUntilExchangeExistsAsync(string exchangeNameStr)
    {
        return WaitUntilFuncAsync(() =>
        {
            return s_httpApiClient.CheckExchangeAsync(exchangeNameStr, checkExisting: true);
        });
    }

    protected static Task WaitUntilExchangeDeletedAsync(IExchangeSpecification exchangeSpec)
    {
        return WaitUntilExchangeDeletedAsync(exchangeSpec.ExchangeName);
    }

    protected static Task WaitUntilExchangeDeletedAsync(string exchangeNameStr)
    {
        return WaitUntilFuncAsync(() =>
        {
            return s_httpApiClient.CheckExchangeAsync(exchangeNameStr, checkExisting: false);
        });
    }

    protected static Task WaitUntilBindingsBetweenExchangeAndQueueExistAsync(
        IExchangeSpecification exchangeSpec, IQueueSpecification queueSpec)
    {
        return WaitUntilBindingsBetweenExchangeAndQueueExistAsync(exchangeSpec.ExchangeName, queueSpec.QueueName);
    }

    protected static Task WaitUntilBindingsBetweenExchangeAndQueueExistAsync(string exchangeNameStr, string queueNameStr)
    {
        return WaitUntilFuncAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr,
                checkExisting: true);
        });
    }

    protected static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(
        IExchangeSpecification exchangeSpec, IQueueSpecification queueSpec)
    {
        return WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(exchangeSpec.ExchangeName, queueSpec.QueueName);
    }

    protected static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(string exchangeNameStr,
        string queueNameStr)
    {
        return WaitUntilFuncAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr,
                checkExisting: false);
        });
    }

    protected static Task WaitUntilBindingsBetweenExchangeAndQueueExistWithArgsAsync(IExchangeSpecification exchangeSpec,
        IQueueSpecification queueSpec,
        Dictionary<string, object> args)
    {
        return WaitUntilBindingsBetweenExchangeAndQueueExistWithArgsAsync(exchangeSpec.ExchangeName,
            queueSpec.QueueName, args);
    }

    protected static Task WaitUntilBindingsBetweenExchangeAndQueueExistWithArgsAsync(string exchangeNameStr,
        string queueNameStr,
        Dictionary<string, object> args)
    {
        return WaitUntilFuncAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr,
                args: args, checkExisting: true);
        });
    }

    protected static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistWithArgsAsync(
        IExchangeSpecification exchangeSpec, IQueueSpecification queueSpec,
        Dictionary<string, object> args)
    {
        return WaitUntilBindingsBetweenExchangeAndQueueDontExistWithArgsAsync(exchangeSpec.ExchangeName,
            queueSpec.QueueName, args);
    }

    protected static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistWithArgsAsync(string exchangeNameStr,
        string queueNameStr,
        Dictionary<string, object> args)
    {
        return WaitUntilFuncAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr,
                args: args, checkExisting: false);
        });
    }

    protected static Task WaitUntilBindingsBetweenExchangeAndExchangeExistAsync(IExchangeSpecification sourceExchangeSpec,
        IExchangeSpecification destinationExchangeSpec)
    {
        return WaitUntilBindingsBetweenExchangeAndExchangeExistAsync(sourceExchangeSpec.ExchangeName,
            destinationExchangeSpec.ExchangeName);
    }

    protected static Task WaitUntilBindingsBetweenExchangeAndExchangeExistAsync(string sourceExchangeNameStr,
        string destinationExchangeNameStr)
    {
        return WaitUntilFuncAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndExchangeAsync(sourceExchangeNameStr,
                destinationExchangeNameStr, checkExisting: true);
        });
    }

    protected static Task WaitUntilBindingsBetweenExchangeAndExchangeDontExistAsync(
        IExchangeSpecification sourceExchangeSpec, IExchangeSpecification destinationExchangeSpec)
    {
        return WaitUntilBindingsBetweenExchangeAndExchangeDontExistAsync(sourceExchangeSpec.ExchangeName,
            destinationExchangeSpec.ExchangeName);
    }

    protected static Task WaitUntilBindingsBetweenExchangeAndExchangeDontExistAsync(string sourceExchangeNameStr,
        string destinationExchangeNameStr)
    {
        return WaitUntilFuncAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndExchangeAsync(sourceExchangeNameStr,
                destinationExchangeNameStr, checkExisting: false);
        });
    }

    protected static Task WaitUntilQueueMessageCount(IQueueSpecification queueSpec, long messageCount)
    {
        return WaitUntilQueueMessageCount(queueSpec.QueueName, messageCount);
    }

    protected static Task WaitUntilQueueMessageCount(string queueNameStr, long messageCount)
    {
        return WaitUntilFuncAsync(async () =>
        {
            EasyNetQ.Management.Client.Model.Queue queue = await s_httpApiClient.GetQueueAsync(queueNameStr);
            return messageCount == queue.MessagesReady;
        }, retries: 30);
    }

    protected static Task DeleteExchangeAsync(string exchangeNameSt)
    {
        return s_httpApiClient.DeleteExchangeAsync(exchangeNameSt);
    }

    private static string InitRabbitMqHost()
    {
        string? envRabbitMqHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST");
        if (false == string.IsNullOrWhiteSpace(envRabbitMqHost))
        {
            return envRabbitMqHost;
        }
        else
        {
            return DefaultRabbitMqHost;
        }
    }

    protected static Task CreateVhostAsync(string vhost)
    {
        return s_httpApiClient.CreateVhostAsync(vhost);
    }

    private static bool InitIsRunningInCI()
    {
        if (bool.TryParse(Environment.GetEnvironmentVariable("CI"), out bool ci))
        {
            if (ci == true)
            {
                return true;
            }
        }
        else if (bool.TryParse(Environment.GetEnvironmentVariable("GITHUB_ACTIONS"), out ci))
        {
            if (ci == true)
            {
                return true;
            }
        }

        return false;
    }

    private static bool InitIsVerbose()
    {
        if (bool.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_CLIENT_TESTS_VERBOSE"), out bool isVerbose))
        {
            return isVerbose;
        }

        return false;
    }

    private static string GenerateShortUuid() => S_Random.Next().ToString("x");
}

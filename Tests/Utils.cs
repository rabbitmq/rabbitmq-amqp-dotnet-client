// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EasyNetQ.Management.Client.Model;
using RabbitMQ.AMQP.Client;
using Xunit.Sdk;

namespace Tests;

public static class SystemUtils
{
    private static readonly HttpApiClient s_httpApiClient = new();
    private static readonly bool s_isRunningInCI = InitIsRunningInCI();
    private static readonly TimeSpan s_initialDelaySpan = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan s_shortDelaySpan = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan s_delaySpan = TimeSpan.FromMilliseconds(500);

    public static bool IsRunningInCI => s_isRunningInCI;

    public static async Task WaitUntilFuncAsync(Func<bool> func, ushort retries = 40)
    {
        await Task.Delay(s_initialDelaySpan);

        while (false == func())
        {
            await Task.Delay(s_shortDelaySpan);

            --retries;
            if (retries == 0)
            {
                throw new XunitException("timed out waiting on a condition!");
            }
        }
    }

    public static async Task WaitUntilAsync(Func<Task<bool>> func, ushort retries = 20)
    {
        if (s_isRunningInCI)
        {
            retries *= 2;
        }

        await Task.Delay(s_initialDelaySpan);

        while (false == await func())
        {
            await Task.Delay(s_delaySpan);

            --retries;
            if (retries == 0)
            {
                throw new XunitException("timed out waiting on a condition!");
            }
        }
    }

    public static Task WaitUntilConnectionIsOpen(string containerId)
    {
        return WaitUntilAsync(() => s_httpApiClient.CheckConnectionAsync(containerId, checkOpened: true));
    }

    public static Task WaitUntilConnectionIsClosed(string containerId)
    {
        return WaitUntilAsync(() => s_httpApiClient.CheckConnectionAsync(containerId, checkOpened: false));
    }

    public static async Task WaitUntilConnectionIsKilled(string containerId)
    {
        await WaitUntilConnectionIsOpen(containerId);
        await WaitUntilAsync(async () => await s_httpApiClient.KillConnectionAsync(containerId) == 1);
    }

    public static async Task WaitUntilConnectionIsKilledAndOpen(string containerId)
    {
        await WaitUntilConnectionIsOpen(containerId);
        await WaitUntilAsync(async () => await s_httpApiClient.KillConnectionAsync(containerId) == 1);
        await WaitUntilConnectionIsOpen(containerId);
    }

    public static Task WaitUntilQueueExistsAsync(IQueueSpecification queueSpec)
    {
        return WaitUntilQueueExistsAsync(queueSpec.Name());
    }

    public static Task WaitUntilQueueExistsAsync(string queueNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckQueueAsync(queueNameStr, checkExisting: true);
        });
    }

    public static Task WaitUntilQueueDeletedAsync(IQueueSpecification queueSpec)
    {
        return WaitUntilQueueDeletedAsync(queueSpec.Name());
    }

    public static Task WaitUntilQueueDeletedAsync(string queueNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckQueueAsync(queueNameStr, checkExisting: false);
        });
    }

    public static Task WaitUntilExchangeExistsAsync(IExchangeSpecification exchangeSpec)
    {
        return WaitUntilExchangeExistsAsync(exchangeSpec.Name());
    }

    public static Task WaitUntilExchangeExistsAsync(string exchangeNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckExchangeAsync(exchangeNameStr, checkExisting: true);
        });
    }

    public static Task WaitUntilExchangeDeletedAsync(IExchangeSpecification exchangeSpec)
    {
        return WaitUntilExchangeDeletedAsync(exchangeSpec.Name());
    }

    public static Task WaitUntilExchangeDeletedAsync(string exchangeNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckExchangeAsync(exchangeNameStr, checkExisting: false);
        });
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueExistAsync(
        IExchangeSpecification exchangeSpec, IQueueSpecification queueSpec)
    {
        return WaitUntilBindingsBetweenExchangeAndQueueExistAsync(exchangeSpec.Name(), queueSpec.Name());
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueExistAsync(string exchangeNameStr, string queueNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr, checkExisting: true);
        });
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(
        IExchangeSpecification exchangeSpec, IQueueSpecification queueSpec)
    {
        return WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(exchangeSpec.Name(), queueSpec.Name());
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(string exchangeNameStr, string queueNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr, checkExisting: false);
        });
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueExistWithArgsAsync(IExchangeSpecification exchangeSpec, IQueueSpecification queueSpec,
        Dictionary<string, object> args)
    {
        return WaitUntilBindingsBetweenExchangeAndQueueExistWithArgsAsync(exchangeSpec.Name(), queueSpec.Name(), args);
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueExistWithArgsAsync(string exchangeNameStr, string queueNameStr,
        Dictionary<string, object> args)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr,
                args: args, checkExisting: true);
        });
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistWithArgsAsync(IExchangeSpecification exchangeSpec, IQueueSpecification queueSpec,
        Dictionary<string, object> args)
    {
        return WaitUntilBindingsBetweenExchangeAndQueueDontExistWithArgsAsync(exchangeSpec.Name(), queueSpec.Name(), args);
    }

    public static Task WaitUntilBindingsBetweenExchangeAndQueueDontExistWithArgsAsync(string exchangeNameStr, string queueNameStr,
        Dictionary<string, object> args)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndQueueAsync(exchangeNameStr, queueNameStr,
                args: args, checkExisting: false);
        });
    }

    public static Task WaitUntilBindingsBetweenExchangeAndExchangeExistAsync(IExchangeSpecification sourceExchangeSpec, IExchangeSpecification destinationExchangeSpec)
    {
        return WaitUntilBindingsBetweenExchangeAndExchangeExistAsync(sourceExchangeSpec.Name(), destinationExchangeSpec.Name());
    }

    public static Task WaitUntilBindingsBetweenExchangeAndExchangeExistAsync(string sourceExchangeNameStr, string destinationExchangeNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndExchangeAsync(sourceExchangeNameStr, destinationExchangeNameStr, checkExisting: true);
        });
    }

    public static Task WaitUntilBindingsBetweenExchangeAndExchangeDontExistAsync(IExchangeSpecification sourceExchangeSpec, IExchangeSpecification destinationExchangeSpec)
    {
        return WaitUntilBindingsBetweenExchangeAndExchangeDontExistAsync(sourceExchangeSpec.Name(), destinationExchangeSpec.Name());
    }

    public static Task WaitUntilBindingsBetweenExchangeAndExchangeDontExistAsync(string sourceExchangeNameStr, string destinationExchangeNameStr)
    {
        return WaitUntilAsync(() =>
        {
            return s_httpApiClient.CheckBindingsBetweenExchangeAndExchangeAsync(sourceExchangeNameStr, destinationExchangeNameStr, checkExisting: false);
        });
    }

    public static Task WaitUntilQueueMessageCount(IQueueSpecification queueSpec, long messageCount)
    {
        return WaitUntilQueueMessageCount(queueSpec.Name(), messageCount);
    }

    public static Task WaitUntilQueueMessageCount(string queueNameStr, long messageCount)
    {
        return WaitUntilAsync(async () =>
        {
            Queue queue = await s_httpApiClient.GetQueueAsync(queueNameStr);
            return messageCount == queue.MessagesReady;
        }, retries: 20);
    }

    public static Task DeleteExchangeAsync(string exchangeNameSt)
    {
        return s_httpApiClient.DeleteExchangeAsync(exchangeNameSt);
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
}

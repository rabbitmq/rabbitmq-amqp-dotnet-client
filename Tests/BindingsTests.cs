// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class BindingsTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    ////////////// ----------------- Bindings TESTS ----------------- //////////////
    [Theory]
    [InlineData("my_source_exchange_!", "my_queue_destination__£$")]
    [InlineData("@@@@@@@!", "~~~~my_queue_destination__;,.£$")]
    [InlineData("التصمي", "~~~~my_التصمي__;,.£$")]
    [InlineData("[7][8][~]他被广泛认为是理论计算机科学和人工智能之父。 ", ",,,£## επιρροή στην ανάπτυξη της")]
    public async Task SimpleBindingsBetweenExchangeAndQueue(string sourceExchange, string queueDestination)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IExchangeSpecification sourceExchangeSpec = _management.Exchange(sourceExchange);
        IQueueSpecification destinationQueueSpec = _management.Queue(queueDestination);

        await sourceExchangeSpec.DeclareAsync();
        await destinationQueueSpec.DeclareAsync();

        IBindingSpecification bindingSpec = _management.Binding()
            .SourceExchange(sourceExchangeSpec)
            .DestinationQueue(destinationQueueSpec)
            .Key("key");
        await bindingSpec.BindAsync();

        await SystemUtils.WaitUntilExchangeExistsAsync(sourceExchangeSpec);

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueExistAsync(sourceExchangeSpec, destinationQueueSpec);

        await bindingSpec.UnbindAsync();

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(sourceExchangeSpec,
            destinationQueueSpec);

        /*
         * TODO dispose assertions?
         */
        await sourceExchangeSpec.DeleteAsync();
        await destinationQueueSpec.DeleteAsync();
        await _connection.CloseAsync();

        await SystemUtils.WaitUntilExchangeDeletedAsync(sourceExchangeSpec);
        await SystemUtils.WaitUntilQueueDeletedAsync(destinationQueueSpec);
    }

    [Fact]
    public async Task BindBetweenExchangeAndQueueTwoTimes()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IExchangeSpecification exchangeSpec = _management.Exchange(_exchangeName);
        IQueueSpecification queueSpec = _management.Queue(_queueName);

        await exchangeSpec.DeclareAsync();
        await queueSpec.DeclareAsync();

        IBindingSpecification firstBindingSpec = _management.Binding()
            .SourceExchange(exchangeSpec)
            .DestinationQueue(queueSpec)
            .Key("first_key");
        IBindingSpecification secondBindingSpec = _management.Binding()
            .SourceExchange(exchangeSpec)
            .DestinationQueue(queueSpec)
            .Key("second_key");

        await firstBindingSpec.BindAsync();
        await secondBindingSpec.BindAsync();

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueExistAsync(exchangeSpec, queueSpec);

        await firstBindingSpec.UnbindAsync();

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueExistAsync(exchangeSpec, queueSpec);

        await secondBindingSpec.UnbindAsync();

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(exchangeSpec, queueSpec);

        await exchangeSpec.DeleteAsync();
        await queueSpec.DeleteAsync();

        await _connection.CloseAsync();

        await SystemUtils.WaitUntilExchangeDeletedAsync(exchangeSpec);
        await SystemUtils.WaitUntilQueueDeletedAsync(queueSpec);
    }


    [Theory]
    [InlineData("source_exchange", "destination_exchange", "mykey")]
    [InlineData("是英国数学家", "是英国数学家", "英国")]
    [InlineData("[7][8][9] 他被广泛认为是理论计算机科学和人工智能之父。 ", "ίχε μεγάλη επιρροή στην ανάπτυξη της",
        "[7][8][9] 他被广泛认为是理论计算机科学和人工智能之父。")]
    [InlineData("ήταν Άγγλος μαθηματικός, επιστήμονας υπολογιστών",
        "ήταν Άγγλος μαθηματικός, επιστήμονας", "επι")]
    [InlineData("(~~~!!++@~./.,€€#!!±§##§¶¡€#¢)",
        "~~~!!++@----.", "==`£!-=+")]

    
    public async Task SimpleBindingsBetweenExchangeAndExchange(string sourceExchangeName,
        string destinationExchangeName,
        string key)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IExchangeSpecification sourceExchangeSpec = _management.Exchange(sourceExchangeName);
        IExchangeSpecification destinationExchangeSpec = _management.Exchange(destinationExchangeName);

        await WhenAllComplete(
            sourceExchangeSpec.DeclareAsync(),
            destinationExchangeSpec.DeclareAsync()
        );

        IBindingSpecification bindingSpecification = _management.Binding()
            .SourceExchange(sourceExchangeSpec)
            .DestinationExchange(destinationExchangeSpec)
            .Key(key);

        await bindingSpecification.BindAsync();

        await SystemUtils.WaitUntilExchangeExistsAsync(sourceExchangeSpec);
        await SystemUtils.WaitUntilBindingsBetweenExchangeAndExchangeExistAsync(sourceExchangeSpec,
            destinationExchangeSpec);

        await bindingSpecification.UnbindAsync();

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndExchangeDontExistAsync(sourceExchangeSpec,
            destinationExchangeSpec);

        await sourceExchangeSpec.DeleteAsync();
        await destinationExchangeSpec.DeleteAsync();
        await _connection.CloseAsync();

        await SystemUtils.WaitUntilExchangeDeletedAsync(sourceExchangeName);
        await SystemUtils.WaitUntilExchangeDeletedAsync(destinationExchangeName);
    }

    [Theory]
    [InlineData("A", 10000, "Z", "是英国数学家")]
    [InlineData("B", 10000L, "H", 0.0001)]
    [InlineData("是英国", 10000.32, "W", 3.0001)]
    [InlineData("是英国", "是英国23", "W", 3.0001)]
    [InlineData("(~~~!!++@----./.,€€#####§¶¡€#¢)","~~~!!++@----", "==`£!-=+", "===£!-=+")]
    public async Task BindingsBetweenExchangeAndQueuesWithArgumentsDifferentValues(string key1, object value1,
        string key2, object value2)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IExchangeSpecification exchangeSpec = _management.Exchange(_exchangeName);
        IQueueSpecification queueSpec = _management.Queue(_queueName);

        await exchangeSpec.DeclareAsync();
        await queueSpec.DeclareAsync();

        await SystemUtils.WaitUntilExchangeExistsAsync(exchangeSpec);
        await SystemUtils.WaitUntilQueueExistsAsync(queueSpec);

        var arguments = new Dictionary<string, object> { { key1, value1 }, { key2, value2 } };
        IBindingSpecification bindingSpecification = _management.Binding()
            .SourceExchange(exchangeSpec)
            .DestinationQueue(queueSpec)
            .Key("key")
            .Arguments(arguments);
        await bindingSpecification.BindAsync();

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueExistWithArgsAsync(
            exchangeSpec,
            queueSpec, arguments);

        await bindingSpecification.UnbindAsync();

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueDontExistWithArgsAsync(
            exchangeSpec,
            queueSpec, arguments);

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueDontExistAsync(exchangeSpec, queueSpec);

        await exchangeSpec.DeleteAsync();
        await queueSpec.DeleteAsync();
        await _connection.CloseAsync();

        await SystemUtils.WaitUntilExchangeDeletedAsync(exchangeSpec);
        await SystemUtils.WaitUntilQueueDeletedAsync(queueSpec);
    }

    // TODO: test with multi-bindings with parameters with list as value
    // The unbinder should be able to unbind the binding with the same arguments
    // The could be a problem when the binding has a list as value

    [Theory]
    [InlineData("my_source_exchange_multi_123", "my_destination_789", "myKey")]
    [InlineData("是英国v_", "destination_是英国v_", "μαθηματικός")]
    [InlineData("(~~~!!++@----./.,€€#####§¶¡€#¢)", ",,~~~!!++@----./.,€€#####§¶¡€#¢@@@", "===£!-=+")]
    public async Task MultiBindingsBetweenExchangeAndQueuesWithArgumentsDifferentValues(string source,
        string destination, string key)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IExchangeSpecification exchangeSpec = _management.Exchange(source);
        IQueueSpecification queueSpec = _management.Queue(destination);

        await WhenAllComplete(exchangeSpec.DeclareAsync(), queueSpec.DeclareAsync());

        // add 10 bindings to have a list of bindings to find
        var bindingSpecTasks = new List<Task>();
        for (int i = 0; i < 10; i++)
        {
            IBindingSpecification bindingSpec = _management.Binding()
                .SourceExchange(exchangeSpec)
                .DestinationQueue(queueSpec)
                .Key(key) // single key to use different args
                .Arguments(new Dictionary<string, object>() { { $"是英国v_{i}", $"p_{i}" } });
            bindingSpecTasks.Add(bindingSpec.BindAsync());
        }

        await WhenAllComplete(bindingSpecTasks);
        bindingSpecTasks.Clear();

        var specialBindArgs =
            new Dictionary<string, object>() { { $"v_8", $"p_8" }, { $"v_1", 1 }, { $"v_r", 1000L }, };
        IBindingSpecification specialBindSpec = _management.Binding()
            .SourceExchange(exchangeSpec)
            .DestinationQueue(queueSpec)
            .Key(key) // single key to use different args
            .Arguments(specialBindArgs);
        await specialBindSpec.BindAsync();

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueExistWithArgsAsync(exchangeSpec, queueSpec,
            specialBindArgs);

        await specialBindSpec.UnbindAsync();

        await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueDontExistWithArgsAsync(exchangeSpec, queueSpec,
            specialBindArgs);

        for (int i = 0; i < 10; i++)
        {
            var bindArgs = new Dictionary<string, object>() { { $"是英国v_{i}", $"p_{i}" } };

            await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueExistWithArgsAsync(exchangeSpec, queueSpec,
                bindArgs);

            await _management.Binding()
                .SourceExchange(exchangeSpec)
                .DestinationQueue(queueSpec)
                .Key(key) // single key to use different args
                .Arguments(bindArgs)
                .UnbindAsync();

            await SystemUtils.WaitUntilBindingsBetweenExchangeAndQueueDontExistWithArgsAsync(exchangeSpec, queueSpec,
                bindArgs);
        }
        await exchangeSpec.DeleteAsync();
        await queueSpec.DeleteAsync();
        await _connection.CloseAsync();
    }
}

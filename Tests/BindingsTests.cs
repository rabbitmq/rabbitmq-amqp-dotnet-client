using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Tests;

public class BindingsTests
{
    ////////////// ----------------- Bindings TESTS ----------------- //////////////
    [Theory]
    [InlineData("my_source_exchange_!", "my_queue_destination__£$")]
    [InlineData("@@@@@@@!", "~~~~my_queue_destination__;,.£$")]
    [InlineData("التصمي", "~~~~my_التصمي__;,.£$")]
    [InlineData("[7][8][~]他被广泛认为是理论计算机科学和人工智能之父。 ", ",,,£## επιρροή στην ανάπτυξη της")]
    public async Task SimpleBindingsBetweenExchangeAndQueue(string sourceExchange, string queueDestination)
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Exchange(sourceExchange).Declare();
        await management.Queue().Name(queueDestination).Declare();
        await management.Binding().SourceExchange(sourceExchange).DestinationQueue(queueDestination)
            .Key("key").Bind();
        SystemUtils.WaitUntil(() => SystemUtils.ExchangeExists(sourceExchange));
        SystemUtils.WaitUntil(() =>
            SystemUtils.BindsBetweenExchangeAndQueueExists(sourceExchange,
                queueDestination));

        await management.Binding().SourceExchange(sourceExchange).DestinationQueue(queueDestination)
            .Key("key").Unbind();

        SystemUtils.WaitUntil(() =>
            !SystemUtils.BindsBetweenExchangeAndQueueExists(sourceExchange,
                queueDestination));

        await management.ExchangeDeletion().Delete(sourceExchange);
        await management.QueueDeletion().Delete(queueDestination);
        await connection.CloseAsync();
        SystemUtils.WaitUntil(() => !SystemUtils.ExchangeExists(sourceExchange));

        await SystemUtils.WaitUntilQueueDeletedAsync(queueDestination);
    }

    [Fact]
    public async Task BindBetweenExchangeAndQueueTwoTimes()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Exchange("exchange_bind_two_times").Declare();
        await management.Queue().Name("queue_bind_two_times").Declare();
        await management.Binding().SourceExchange("exchange_bind_two_times").DestinationQueue("queue_bind_two_times")
            .Key("first_key").Bind();
        await management.Binding().SourceExchange("exchange_bind_two_times").DestinationQueue("queue_bind_two_times")
            .Key("second_key").Bind();
        SystemUtils.WaitUntil(() =>
            SystemUtils.BindsBetweenExchangeAndQueueExists("exchange_bind_two_times",
                "queue_bind_two_times"));

        await management.Binding().SourceExchange("exchange_bind_two_times").DestinationQueue("queue_bind_two_times")
            .Key("first_key")
            .Unbind();

        SystemUtils.WaitUntil(() =>
            SystemUtils.BindsBetweenExchangeAndQueueExists("exchange_bind_two_times",
                "queue_bind_two_times"));

        await management.Binding().SourceExchange("exchange_bind_two_times").DestinationQueue("queue_bind_two_times")
            .Key("second_key")
            .Unbind();

        SystemUtils.WaitUntil(() => !SystemUtils.BindsBetweenExchangeAndQueueExists("exchange_bind_two_times",
            "queue_bind_two_times"));

        await management.ExchangeDeletion().Delete("exchange_bind_two_times");

        await management.QueueDeletion().Delete("queue_bind_two_times");
        await connection.CloseAsync();

        SystemUtils.WaitUntil(() => !SystemUtils.ExchangeExists("exchange_bind_two_times"));
        await SystemUtils.WaitUntilQueueDeletedAsync("queue_bind_two_times");
    }


    [Theory]
    [InlineData("source_exchange", "destination_exchange", "mykey")]
    [InlineData("是英国数学家", "是英国数学家", "英国")]
    [InlineData("[7][8][9] 他被广泛认为是理论计算机科学和人工智能之父。 ", "ίχε μεγάλη επιρροή στην ανάπτυξη της",
        "[7][8][9] 他被广泛认为是理论计算机科学和人工智能之父。")]
    [InlineData("ήταν Άγγλος μαθηματικός, επιστήμονας υπολογιστών",
        "ήταν Άγγλος μαθηματικός, επιστήμονας", "επι")]
    public async Task SimpleBindingsBetweenExchangeAndExchange(string sourceExchange, string destinationExchange,
        string key)
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Exchange(sourceExchange).Declare();
        await management.Exchange(destinationExchange).Declare();
        await management.Binding().SourceExchange(sourceExchange)
            .DestinationExchange(destinationExchange)
            .Key(key).Bind();
        SystemUtils.WaitUntil(() => SystemUtils.ExchangeExists(sourceExchange));

        SystemUtils.WaitUntil(() =>
            SystemUtils.BindsBetweenExchangeAndExchangeExists(sourceExchange,
                destinationExchange));

        await management.Binding().SourceExchange(sourceExchange)
            .DestinationExchange(destinationExchange)
            .Key(key).Unbind();

        SystemUtils.WaitUntil(() =>
            !SystemUtils.BindsBetweenExchangeAndExchangeExists(sourceExchange,
                destinationExchange));

        await management.ExchangeDeletion().Delete(sourceExchange);
        await management.ExchangeDeletion().Delete(destinationExchange);
        await connection.CloseAsync();
        SystemUtils.WaitUntil(() => !SystemUtils.ExchangeExists(sourceExchange));
        SystemUtils.WaitUntil(() => !SystemUtils.ExchangeExists(destinationExchange));
    }

    [Theory]
    [InlineData("A", 10000, "Z", "是英国数学家")]
    [InlineData("B", 10000L, "H", 0.0001)]
    [InlineData("是英国", 10000.32, "W", 3.0001)]
    [InlineData("是英国", "是英国23", "W", 3.0001)]
    public async Task BindingsBetweenExchangeAndQueuesWithArgumentsDifferentValues(string key1, object value1,
        string key2, object value2)
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Exchange("exchange_bindings_with_arguments").Declare();
        await management.Queue().Name("queue_bindings_with_arguments").Declare();
        var arguments = new Dictionary<string, object> { { key1, value1 }, { key2, value2 } };
        await management.Binding().SourceExchange("exchange_bindings_with_arguments")
            .DestinationQueue("queue_bindings_with_arguments")
            .Key("key")
            .Arguments(arguments)
            .Bind();
        SystemUtils.WaitUntil(() => SystemUtils.ExchangeExists("exchange_bindings_with_arguments"));
        SystemUtils.WaitUntil(() =>
            SystemUtils.ArgsBindsBetweenExchangeAndQueueExists("exchange_bindings_with_arguments",
                "queue_bindings_with_arguments", arguments));
        await management.Binding().SourceExchange("exchange_bindings_with_arguments")
            .DestinationQueue("queue_bindings_with_arguments")
            .Key("key").Arguments(arguments).Unbind();
        SystemUtils.WaitUntil(() =>
            !SystemUtils.ArgsBindsBetweenExchangeAndQueueExists("exchange_bindings_with_arguments",
                "queue_bindings_with_arguments", arguments));

        SystemUtils.WaitUntil(() =>
            !SystemUtils.BindsBetweenExchangeAndQueueExists("exchange_bindings_with_arguments",
                "queue_bindings_with_arguments"));

        await management.ExchangeDeletion().Delete("exchange_bindings_with_arguments");
        await management.QueueDeletion().Delete("queue_bindings_with_arguments");
        await connection.CloseAsync();

        SystemUtils.WaitUntil(() => !SystemUtils.ExchangeExists("exchange_bindings_with_arguments"));
        await SystemUtils.WaitUntilQueueDeletedAsync("queue_bindings_with_arguments");
    }

    // TODO: test with multi-bindings with parameters with list as value
    // The unbinder should be able to unbind the binding with the same arguments
    // The could be a problem when the binding has a list as value

    [Theory]
    [InlineData("my_source_exchange_multi_123", "my_destination_789", "myKey")]
    [InlineData("是英国v_", "destination_是英国v_", "μαθηματικός")]
    // TODO: to validate.  Atm it seems there is a server side problem
    // [InlineData("(~~~!!++@----./.,€€#####§¶¡€#¢)", ",,~~~!!++@----./.,€€#####§¶¡€#¢@@@", "===£!-=+")]
    public async Task MultiBindingsBetweenExchangeAndQueuesWithArgumentsDifferentValues(string source,
        string destination, string key)
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Exchange(source).Declare();
        await management.Queue().Name(destination).Declare();
        // add 10 bindings to have a list of bindings to find
        for (int i = 0; i < 10; i++)
        {
            await management.Binding().SourceExchange(source)
                .DestinationQueue(destination)
                .Key(key) // single key to use different args
                .Arguments(new Dictionary<string, object>() { { $"是英国v_{i}", $"p_{i}" } })
                .Bind();
        }

        var specialBind = new Dictionary<string, object>() { { $"v_8", $"p_8" }, { $"v_1", 1 }, { $"v_r", 1000L }, };
        await management.Binding().SourceExchange(source)
            .DestinationQueue(destination)
            .Key(key) // single key to use different args
            .Arguments(specialBind)
            .Bind();
        SystemUtils.WaitUntil(() =>
            SystemUtils.ArgsBindsBetweenExchangeAndQueueExists(source,
                destination, specialBind));

        await management.Binding().SourceExchange(source).DestinationQueue(destination).Key(key).Arguments(specialBind)
            .Unbind();

        SystemUtils.WaitUntil(() =>
            !SystemUtils.ArgsBindsBetweenExchangeAndQueueExists(source,
                destination, specialBind));

        for (int i = 0; i < 10; i++)
        {
            var b = new Dictionary<string, object>() { { $"是英国v_{i}", $"p_{i}" } };
            SystemUtils.WaitUntil(() =>
                SystemUtils.ArgsBindsBetweenExchangeAndQueueExists(source,
                    destination, b));
            await management.Binding().SourceExchange(source)
                .DestinationQueue(destination)
                .Key(key) // single key to use different args
                .Arguments(b)
                .Unbind();

            SystemUtils.WaitUntil(() =>
                !SystemUtils.ArgsBindsBetweenExchangeAndQueueExists(source,
                    destination, b));
        }

        await management.ExchangeDeletion().Delete(source);
        await management.QueueDeletion().Delete(destination);
        await connection.CloseAsync();
    }
}

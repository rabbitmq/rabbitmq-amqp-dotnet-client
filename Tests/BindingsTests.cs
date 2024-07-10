using System.Threading.Tasks;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Tests;

public class BindingsTests
{
    ////////////// ----------------- Bindings TESTS ----------------- //////////////


    [Fact]
    public async Task SimpleBindingsBetweenExchangeAndQueue()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        await connection.ConnectAsync();
        var management = connection.Management();
        await management.Exchange("exchange_simple_bindings").Declare();
        await management.Queue().Name("queue_simple_bindings").Declare();
        await management.Binding().SourceExchange("exchange_simple_bindings").DestinationQueue("queue_simple_bindings")
            .Key("key").Bind();
        SystemUtils.WaitUntil(() => SystemUtils.ExchangeExists("exchange_simple_bindings"));
        SystemUtils.WaitUntil(() =>
            SystemUtils.BindsBetweenExchangeAndQueueExists("exchange_simple_bindings",
                "queue_simple_bindings"));

        await management.Unbind().SourceExchange("exchange_simple_bindings").DestinationQueue("queue_simple_bindings")
            .Key("key").UnBind();

        SystemUtils.WaitUntil(() =>
            !SystemUtils.BindsBetweenExchangeAndQueueExists("exchange_simple_bindings",
                "queue_simple_bindings"));

        await management.ExchangeDeletion().Delete("exchange_simple_bindings");
        await management.QueueDeletion().Delete("queue_simple_bindings");
        await connection.CloseAsync();
        SystemUtils.WaitUntil(() => !SystemUtils.ExchangeExists("exchange_simple_bindings"));
        SystemUtils.WaitUntil(() => !SystemUtils.QueueExists("queue_simple_bindings"));
    }

    [Fact]
    public async Task BindBetweenExchangeAndQueueTwoTimes()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        await connection.ConnectAsync();
        var management = connection.Management();
        await management.Exchange("exchange_bind_two_times").Declare();
        await management.Queue().Name("queue_bind_two_times").Declare();


        await management.Binding().SourceExchange("exchange_bind_two_times").DestinationQueue("queue_bind_two_times")
            .Key("first_key").Bind();

        await management.Binding().SourceExchange("exchange_bind_two_times").DestinationQueue("queue_bind_two_times")
            .Key("second_key").Bind();


        SystemUtils.WaitUntil(() =>
            SystemUtils.BindsBetweenExchangeAndQueueExists("exchange_bind_two_times",
                "queue_bind_two_times"));

        await management.Unbind().SourceExchange("exchange_bind_two_times").DestinationQueue("queue_bind_two_times")
            .Key("first_key")
            .UnBind();

        SystemUtils.WaitUntil(() =>
            SystemUtils.BindsBetweenExchangeAndQueueExists("exchange_bind_two_times",
                "queue_bind_two_times"));

        await management.Unbind().SourceExchange("exchange_bind_two_times").DestinationQueue("queue_bind_two_times")
            .Key("second_key")
            .UnBind();

        SystemUtils.WaitUntil(() => !SystemUtils.BindsBetweenExchangeAndQueueExists("exchange_bind_two_times",
            "queue_bind_two_times"));

        await management.ExchangeDeletion().Delete("exchange_bind_two_times");

        await management.QueueDeletion().Delete("queue_bind_two_times");
        await connection.CloseAsync();
        SystemUtils.WaitUntil(() => !SystemUtils.ExchangeExists("exchange_bind_two_times"));
        SystemUtils.WaitUntil(() => !SystemUtils.QueueExists("queue_bind_two_times"));
    }


    [Fact]
    public async Task SimpleBindingsBetweenExchangeAndExchange()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        await connection.ConnectAsync();
        var management = connection.Management();
        await management.Exchange("exchange_simple_bindings").Declare();
        await management.Exchange("exchange_simple_bindings_destination").Declare();
        await management.Binding().SourceExchange("exchange_simple_bindings")
            .DestinationExchange("exchange_simple_bindings_destination")
            .Key("key").Bind();
        SystemUtils.WaitUntil(() => SystemUtils.ExchangeExists("exchange_simple_bindings"));

        SystemUtils.WaitUntil(() =>
            SystemUtils.BindsBetweenExchangeAndExchangeExists("exchange_simple_bindings",
                "exchange_simple_bindings_destination"));


        await management.Unbind().SourceExchange("exchange_simple_bindings")
            .DestinationExchange("exchange_simple_bindings_destination")
            .Key("key").UnBind();

        SystemUtils.WaitUntil(() =>
            !SystemUtils.BindsBetweenExchangeAndExchangeExists("exchange_simple_bindings",
                "exchange_simple_bindings_destination"));

        await management.ExchangeDeletion().Delete("exchange_simple_bindings");
        await management.ExchangeDeletion().Delete("exchange_simple_bindings_destination");
        await connection.CloseAsync();
        SystemUtils.WaitUntil(() => !SystemUtils.ExchangeExists("exchange_simple_bindings"));
        SystemUtils.WaitUntil(() => !SystemUtils.ExchangeExists("exchange_simple_bindings_destination"));
    }
}

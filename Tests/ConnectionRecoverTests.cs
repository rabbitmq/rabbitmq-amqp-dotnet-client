using System;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;

namespace Tests;

using Xunit;

public class ConnectionRecoverTests
{
    [Fact]
    public async void UnexpectedClose()
    {
        AmqpConnection connection = new();

        var completion = new TaskCompletionSource<bool>();
        connection.Closed += (sender, unexpected) =>
        {
            Assert.Equal(Status.Closed, connection.Status);
            completion.SetResult(unexpected);
        };

        var connectionName = Guid.NewGuid().ToString();
        await connection.ConnectAsync(new AmqpAddressBuilder().ConnectionName(connectionName).Build());
        SystemUtils.WaitUntilAsync(async () => await SystemUtils.ConnectionsCountByName(connectionName) == 1);
        SystemUtils.WaitUntilAsync(async () => await SystemUtils.HttpKillConnections(connectionName) == 1);

        var result = await completion.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(result);
        Assert.Equal(Status.Closed, connection.Status);
        await connection.CloseAsync();
    }
}
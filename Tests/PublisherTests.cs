using System;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class PublisherTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public async Task ValidateBuilderRaiseExceptionIfQueueOrExchangeAreNotSetCorrectly()
    {
        Assert.NotNull(_connection);

        await Assert.ThrowsAsync<InvalidAddressException>(() =>
            _connection.PublisherBuilder().Queue("does_not_matter").Exchange("i_should_not_stay_here").BuildAsync());

        await Assert.ThrowsAsync<InvalidAddressException>(() => _connection.PublisherBuilder().Exchange("").BuildAsync());

        await Assert.ThrowsAsync<InvalidAddressException>(() => _connection.PublisherBuilder().Queue("").BuildAsync());

        Assert.Empty(_connection.GetPublishers());

        await _connection.CloseAsync();
    }

    [Fact]
    public async Task PublisherShouldThrowWhenQueueDoesNotExist()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        string doesNotExist = Guid.NewGuid().ToString();

        PublisherException ex = await Assert.ThrowsAsync<PublisherException>(() =>
            _connection.PublisherBuilder().Queue(doesNotExist).BuildAsync());

        Assert.Contains(doesNotExist, ex.Message);
    }

    [Fact]
    public async Task PublisherShouldThrowWhenExchangeDoesNotExist()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        string doesNotExist = Guid.NewGuid().ToString();

        PublisherException ex = await Assert.ThrowsAsync<PublisherException>(() =>
            _connection.PublisherBuilder().Exchange(doesNotExist).BuildAsync());

        Assert.Contains(doesNotExist, ex.Message);
    }

    [Fact]
    public async Task SendAMessageToAQueue()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpecification = _management.Queue(_queueName);
        await queueSpecification.DeclareAsync();

        IPublisher publisher = await _connection.PublisherBuilder().Queue(queueSpecification).BuildAsync();

        PublishResult pr = await publisher.PublishAsync(new AmqpMessage("Hello wold!"));
        Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);

        await SystemUtils.WaitUntilQueueMessageCount(queueSpecification, 1);

        Assert.Single(_connection.GetPublishers());
        await publisher.CloseAsync();
        Assert.Empty(_connection.GetPublishers());
    }


    [Fact]
    public async Task ValidatePublishersCount()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue(_queueName);
        await queueSpec.DeclareAsync();

        for (int i = 1; i <= 10; i++)
        {
            IPublisher publisher = await _connection.PublisherBuilder().Queue(queueSpec).BuildAsync();

            PublishResult pr = await publisher.PublishAsync(new AmqpMessage("Hello wold!"));
            Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);
            Assert.Equal(i, _connection.GetPublishers().Count);
        }

        foreach (IPublisher publisher in _connection.GetPublishers())
        {
            await publisher.CloseAsync();
        }

        await _connection.CloseAsync();
        Assert.Empty(_connection.GetPublishers());
    }

    [Fact]
    public async Task SendAMessageToAnExchange()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueToSend1 = _management.Queue(_queueName);
        IExchangeSpecification exchangeToSend = _management.Exchange(_exchangeName);

        await queueToSend1.DeclareAsync();
        await exchangeToSend.DeclareAsync();

        IBindingSpecification bindingSpec = _management.Binding()
            .SourceExchange(exchangeToSend)
            .DestinationQueue(queueToSend1)
            .Key("key");
        await bindingSpec.BindAsync();

        IPublisher publisher = await _connection.PublisherBuilder()
            .Exchange(exchangeToSend).Key("key").BuildAsync();

        PublishResult pr = await publisher.PublishAsync(new AmqpMessage("Hello wold!"));
        Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);

        await SystemUtils.WaitUntilQueueMessageCount(queueToSend1, 1);

        Assert.Single(_connection.GetPublishers());
        await publisher.CloseAsync();
        Assert.Empty(_connection.GetPublishers());

        await bindingSpec.UnbindAsync();
    }

    [Fact]
    public async Task PublisherSendingShouldThrowWhenExchangeHasBeenDeleted()
    {
        /*
         * TODO
         * Note: this test is a little different than the Java client
         * The Java client has a dedicated "entity not found" exception
         */
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        IMessage message = new AmqpMessage(Encoding.ASCII.GetBytes("hello"));

        IExchangeSpecification exchangeSpecification = _management.Exchange(_exchangeName).Type(ExchangeType.FANOUT);
        await exchangeSpecification.DeclareAsync();

        IPublisherBuilder publisherBuilder = _connection.PublisherBuilder();
        // TODO implement Listeners
        IPublisher publisher = await publisherBuilder.Exchange(_exchangeName).BuildAsync();

        try
        {
            IQueueSpecification queueSpecification = _management.Queue().Exclusive(true);
            IQueueInfo queueInfo = await queueSpecification.DeclareAsync();
            IBindingSpecification bindingSpecification = _management.Binding()
                .SourceExchange(_exchangeName)
                .DestinationQueue(queueSpecification);
            await bindingSpecification.BindAsync();

            PublishResult publishResult = await publisher.PublishAsync(message);
            Assert.Equal(OutcomeState.Accepted, publishResult.Outcome.State);
        }
        finally
        {
            await exchangeSpecification.DeleteAsync();
        }

        PublishOutcome? publishOutcome = null;
        for (int i = 0; i < 100; i++)
        {
            PublishResult nextPublishResult = await publisher.PublishAsync(message);
            if (OutcomeState.Failed == nextPublishResult.Outcome.State)
            {
                publishOutcome = nextPublishResult.Outcome;
                break;
            }
            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }

        Assert.NotNull(publishOutcome);
        Assert.NotNull(publishOutcome.Error);
        Assert.Contains(_exchangeName, publishOutcome.Error.Description);
        Assert.Equal("amqp:not-found", publishOutcome.Error.ErrorCode);
    }
}

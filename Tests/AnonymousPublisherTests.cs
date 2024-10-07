using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class AnonymousPublisherTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
    {
        [Fact]
        public async Task AnonymousPublisherPublishResultNotAcceptedWhenQueueDoesNotExist()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            IPublisher aPublisher = await _connection.PublisherBuilder().BuildAsync();
            PublishResult pr = await aPublisher.PublishAsync(
                new AmqpMessage("Hello, World!").ToAddress().Queue("DoesNotExist").Build());
            Assert.Equal(OutcomeState.Released, pr.Outcome.State);
        }

        [Fact]
        public async Task AnonymousPublisherPublishResultNotAcceptedWhenExchangeDoesNotExist()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            IPublisher aPublisher = await _connection.PublisherBuilder().BuildAsync();
            PublishResult pr = await aPublisher.PublishAsync(
                new AmqpMessage("Hello, World!").ToAddress().Exchange("DoesNotExist").Build());
            Assert.Equal(OutcomeState.Released, pr.Outcome.State);
        }

        [Fact]
        public async Task AnonymousPublisherPublishResultAcceptedWhenQueueExists()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            await _management.Queue(_queueName).Quorum().Queue().DeclareAsync();
            IPublisher aPublisher = await _connection.PublisherBuilder().BuildAsync();
            PublishResult pr = await aPublisher.PublishAsync(
                new AmqpMessage("Hello, World!").ToAddress().Queue(_queueName).Build());
            Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);
            await aPublisher.CloseAsync();
        }

        [Theory]
        [InlineData(OutcomeState.Accepted, "myValidKey")]
        [InlineData(OutcomeState.Released, "myInvalidKey")]
        public async Task AnonymousPublisherPublishResultAcceptedWhenExchangeExists(OutcomeState outcomeState, string key)
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            await _management.Exchange(_exchangeName).Type(ExchangeType.TOPIC).DeclareAsync();
            await _management.Queue(_queueName).Quorum().Queue().DeclareAsync();
            await _management.Binding().SourceExchange(_exchangeName).DestinationQueue(_queueName).Key(key).BindAsync();
            IPublisher aPublisher = await _connection.PublisherBuilder().BuildAsync();
            PublishResult pr = await aPublisher.PublishAsync(
                new AmqpMessage("Hello, World!").ToAddress().Exchange(_exchangeName).Key("myValidKey").Build());
            Assert.Equal(outcomeState, pr.Outcome.State);
            await aPublisher.CloseAsync();
        }
    }
}

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

            // TODO: why is this sometimes "Rejected"?
            Assert.True(OutcomeState.Released == pr.Outcome.State || OutcomeState.Rejected == pr.Outcome.State);
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

        /// <summary>
        /// Test when the exchange exists and the key is valid.
        /// Released when the key is invalid.
        /// </summary>
        /// <param name="outcomeState"></param>
        /// <param name="key"></param>
        [Theory]
        [InlineData(OutcomeState.Accepted, "myValidKey")]
        [InlineData(OutcomeState.Released, "myInvalidKey")]
        public async Task AnonymousPublisherPublishResultAcceptedWhenExchangeExists(OutcomeState outcomeState,
            string key)
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

        /// <summary>
        /// In this test, we are sending a message to a queue that is different from the one defined in the address.
        /// So we mix anonymous and defined queues.
        /// The winner is the defined queue. So the 
        /// </summary>
        [Fact]
        public async Task MessageShouldGoToTheDefinedQueueAndNotToTheAddressTo()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);
            await _management.Queue(_queueName).Quorum().Queue().DeclareAsync();
            await _management.Queue(_queueName + "2").Quorum().Queue().DeclareAsync();

            IPublisher publisher = await _connection.PublisherBuilder().Queue(_queueName).BuildAsync();
            PublishResult pr =
                await publisher.PublishAsync(new AmqpMessage("Hello, World!").ToAddress().Queue(_queueName + "2")
                    .Build());
            Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);
            await Task.Delay(200);
            await SystemUtils.WaitUntilQueueMessageCount(_queueName, 1);
            await SystemUtils.WaitUntilQueueMessageCount(_queueName + "2", 0);
            await _management.Queue(_queueName + "2").Quorum().Queue().DeleteAsync();
        }
    }
}

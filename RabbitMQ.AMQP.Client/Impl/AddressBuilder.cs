namespace RabbitMQ.AMQP.Client.Impl
{
    public class AddressBuilder : IAddressBuilder<AddressBuilder>
    {
        private string? _exchange;

        private string? _queue;

        private string? _key;

        public AddressBuilder Exchange(IExchangeSpecification exchangeSpec)
        {
            return Exchange(exchangeSpec.ExchangeName);
        }

        public AddressBuilder Exchange(string? exchange)
        {
            _exchange = exchange;
            return this;
        }

        public AddressBuilder Queue(IQueueSpecification queueSpec)
        {
            return Queue(queueSpec.QueueName);
        }

        public AddressBuilder Queue(string? queue)
        {
            _queue = queue;
            return this;
        }

        public AddressBuilder Key(string? key)
        {
            _key = key;
            return this;
        }

        public string Address()
        {
            if (_exchange == null && _queue == null)
            {
                throw new InvalidAddressException("Exchange or Queue must be set");
            }

            if (_exchange != null && _queue != null)
            {
                throw new InvalidAddressException("Exchange and Queue cannot be set together");
            }

            if (_exchange != null)
            {
                if (string.IsNullOrEmpty(_exchange))
                {
                    throw new InvalidAddressException("Exchange must be set");
                }

                if (_key != null && false == string.IsNullOrEmpty(_key))
                {
                    return $"/{Consts.Exchanges}/{Utils.EncodePathSegment(_exchange)}/{Utils.EncodePathSegment(_key)}";
                }

                return $"/{Consts.Exchanges}/{Utils.EncodePathSegment(_exchange)}";
            }

            if (_queue == null)
            {
                return "";
            }

            if (string.IsNullOrEmpty(_queue))
            {
                throw new InvalidAddressException("Queue must be set");
            }

            return $"/{Consts.Queues}/{Utils.EncodePathSegment(_queue)}";
        }
    }
}

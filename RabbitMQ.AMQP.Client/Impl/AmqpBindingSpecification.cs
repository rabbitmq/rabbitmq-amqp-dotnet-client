using Amqp;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpBindingSpecification(AmqpManagement management) : IBindingSpecification
{
    private AmqpManagement Management { get; } = management;
    private string _source = "";
    private string _destination = "";
    private string _routingKey = "";
    private bool _toQueue = true;


    public async Task Bind()
    {
        // if (string.IsNullOrEmpty(_source))
        // {
        //     throw new ArgumentException("Source must be set");
        // }
        //
        // if (string.IsNullOrEmpty(_destination))
        // {
        //     throw new ArgumentException("Destination must be set");
        // }
        //
        // if (string.IsNullOrEmpty(_routingKey))
        // {
        //     throw new ArgumentException("Routing key must be set");
        // }
        // Map<String, Object> body = new LinkedHashMap<>();
        // body.put("source", this.state.source);
        // body.put("binding_key", this.state.key == null ? "" : this.state.key);
        // body.put("arguments", this.state.arguments);
        // if (this.state.toQueue) {
        //     body.put("destination_queue", this.state.destination);
        //     this.state.managememt.bind(body);
        // } else {
        //     body.put("destination_exchange", this.state.destination);
        //     this.state.managememt.bind(body);
        // }

        var kv = new Map
        {
            { "source", _source },
            { "binding_key", _routingKey },
            { "arguments", new Map() },
            { _toQueue ? "destination_queue" : "destination_exchange", _destination }
        };

        await Management.Request(kv, $"/{Consts.Bindings}",
            AmqpManagement.Post,
            [
                AmqpManagement.Code204,
            ]).ConfigureAwait(false);
    }

    public IBindingSpecification SourceExchange(string exchange)
    {
        _toQueue = false;
        _source = exchange;
        return this;
    }

    public IBindingSpecification DestinationQueue(string queue)
    {
        _toQueue = true;
        _destination = queue;
        return this;
    }

    public IBindingSpecification DestinationExchange(string exchange)
    {
        _destination = exchange;
        return this;
    }

    public IBindingSpecification Key(string key)
    {
        _routingKey = key;
        return this;
    }

    public IBindingSpecification Argument(string key, object value)
    {
        return this;
    }

    public IBindingSpecification Arguments(Dictionary<string, object> arguments) => throw new NotImplementedException();
}

public class AmqpUnBindingSpecification(AmqpManagement management) : IUnbindSpecification
{
    private AmqpManagement Management { get; } = management;
    private string _source = "";
    private string _destination = "";
    private string _routingKey = "";
    private bool _toQueue = true;



    public IUnbindSpecification SourceExchange(string exchange)
    {
        _source = exchange;
        return this;
    }

    public IUnbindSpecification DestinationQueue(string queue)
    {
        _toQueue = true;
        _destination = queue;
        return this;
    }

    public IUnbindSpecification DestinationExchange(string exchange)
    {
        _toQueue = false;
        _destination = exchange;
        return this;
    }

    public IUnbindSpecification Key(string key)
    {
        _routingKey = key;
        return this;
    }

    public IUnbindSpecification Argument(string key, object value) => throw new NotImplementedException();

    public IUnbindSpecification Arguments(Dictionary<string, object> arguments) => throw new NotImplementedException();

    public async Task UnBind()
    {
        // String target =
        //     "/bindings/"
        //     + "src="
        //     + UriUtils.encodePathSegment(source)
        //     + ";"
        //     + destinationField
        //     + "="
        //     + UriUtils.encodePathSegment(destination)
        //     + ";key="
        //     + UriUtils.encodePathSegment(key)
        //     + ";args=";

        string destinationCharacter = _toQueue ? "dstq" : "dste";

        string target =
            $"/{Consts.Bindings}/src={_source};" +
            $"{($"{destinationCharacter}={_destination};key={_routingKey};args=")}";

        await Management.Request(
            null, target, 
            AmqpManagement.Delete, new[] { AmqpManagement.Code204 }).ConfigureAwait(false);
    }
}

using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

public abstract class BindingSpecificationBase
{
    protected string Source = "";
    protected string Destination = "";
    protected string RoutingKey = "";
    protected bool ToQueue = true;
    protected Dictionary<string, object> _arguments = new();

    protected Map ArgsToMap()
    {
        Map argMap = new();

        foreach ((string key, object value) in _arguments)
        {
            argMap[key] = value;
        }

        return argMap;
    }
}

public class AmqpBindingSpecification : BindingSpecificationBase, IBindingSpecification
{
    private readonly AmqpManagement _management;

    public AmqpBindingSpecification(AmqpManagement management)
    {
        _management = management;
    }

    public Dictionary<string, object> Arguments()
    {
        return _arguments;
    }

    public string Path() => BindingsTarget();

    public async Task BindAsync()
    {
        var kv = new Map
        {
            { "source", Source },
            { "binding_key", RoutingKey },
            { "arguments", ArgsToMap() },
            { ToQueue ? "destination_queue" : "destination_exchange", Destination }
        };

        string path = $"/{Consts.Bindings}";
        string method = AmqpManagement.Post;
        int[] expectedReturnCodes = [AmqpManagement.Code204];
        // Note: must use await so that ConfigureAwait(false) can be called
        _management.TopologyListener().BindingDeclared(this);
        await _management.RequestAsync(kv, path, method, expectedReturnCodes)
            .ConfigureAwait(false);
    }

    public async Task UnbindAsync()
    {
        string method = AmqpManagement.Delete;
        string destinationCharacter = ToQueue ? "dstq" : "dste";
        int[] expectedReturnCodes = [AmqpManagement.Code204];

        if (_arguments.Count == 0)
        {
            string path =
                $"/{Consts.Bindings}/src={Utils.EncodePathSegment(Source)};" +
                $"{($"{destinationCharacter}={Utils.EncodePathSegment(Destination)};" +
                    $"key={Utils.EncodePathSegment(RoutingKey)};args=")}";

            _management.TopologyListener().BindingDeleted(Path());
            await _management.RequestAsync(null, path, method, expectedReturnCodes)
                .ConfigureAwait(false);
        }
        else
        {
            string bindingsPath = BindingsTarget();
            List<Map> bindings = await GetBindings(bindingsPath).ConfigureAwait(false);
            string? path = MatchBinding(bindings, RoutingKey, ArgsToMap());
            if (path is null)
            {
                // TODO is this an error?
            }
            else
            {
                _management.TopologyListener().BindingDeclared(this);
                await _management.RequestAsync(null, path, method, expectedReturnCodes)
                    .ConfigureAwait(false);
            }
        }
    }

    public IBindingSpecification SourceExchange(IExchangeSpecification exchangeSpec)
    {
        return SourceExchange(exchangeSpec.Name());
    }

    public IBindingSpecification SourceExchange(string exchangeName)
    {
        ToQueue = false;
        Source = exchangeName;
        return this;
    }

    public string SourceExchangeName()
    {
        return Source;
    }

    public IBindingSpecification DestinationQueue(IQueueSpecification queueSpec)
    {
        return DestinationQueue(queueSpec.Name());
    }

    public IBindingSpecification DestinationQueue(string queueName)
    {
        ToQueue = true;
        Destination = queueName;
        return this;
    }

    public string DestinationQueueName() => Destination;

    public IBindingSpecification DestinationExchange(IExchangeSpecification exchangeSpec)
    {
        return DestinationExchange(exchangeSpec.Name());
    }

    public IBindingSpecification DestinationExchange(string exchangeName)
    {
        Destination = exchangeName;
        return this;
    }

    public string DestinationExchangeName() => Destination;

    public IBindingSpecification Key(string key)
    {
        RoutingKey = key;
        return this;
    }

    public string Key() => RoutingKey;

    public IBindingSpecification Argument(string key, object value)
    {
        _arguments[key] = value;
        return this;
    }

    public IBindingSpecification Arguments(Dictionary<string, object> arguments)
    {
        _arguments = arguments;
        return this;
    }

    private string BindingsTarget()
    {
        string destinationField = ToQueue ? "dstq" : "dste";
        return "/bindings?src="
               + Utils.EncodeHttpParameter(Source)
               + "&"
               + destinationField
               + "="
               + Utils.EncodeHttpParameter(Destination)
               + "&key="
               + Utils.EncodeHttpParameter(RoutingKey);
    }

    private async Task<List<Map>> GetBindings(string path)
    {
        Amqp.Message result = await _management.RequestAsync(
            null, path,
            AmqpManagement.Get, new[] { AmqpManagement.Code200 }).ConfigureAwait(false);

        if (result.Body is not List list)
        {
            return [];
        }

        var l = new List<Map>() { };
        foreach (object o in list)
        {
            if (o is Map item)
            {
                l.Add(item);
            }
        }

        return l;
    }

    private string? MatchBinding(List<Map> bindings, string key, Map arguments)
    {
        string? uri = null;
        foreach (Map binding in bindings)
        {
            string bindingKey = (string)binding["binding_key"];
            Map bindingArguments = (Map)binding["arguments"];
            if ((key == null && bindingKey == null) || (key != null && key.Equals(bindingKey)))
            {
                if ((arguments == null && bindingArguments == null) ||
                    (arguments != null && Utils.CompareMap(arguments, bindingArguments)))
                {
                    uri = binding["location"].ToString();
                    break;
                }
            }
        }

        return uri;
    }
}

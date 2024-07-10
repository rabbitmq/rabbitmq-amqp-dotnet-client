using Amqp;
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

public class AmqpBindingSpecification(AmqpManagement management) : BindingSpecificationBase, IBindingSpecification
{
    private AmqpManagement Management { get; } = management;

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

        Map argMap = ArgsToMap();

        var kv = new Map
        {
            { "source", Source },
            { "binding_key", RoutingKey },
            { "arguments", argMap },
            { ToQueue ? "destination_queue" : "destination_exchange", Destination }
        };

        await Management.Request(kv, $"/{Consts.Bindings}",
            AmqpManagement.Post,
            [
                AmqpManagement.Code204,
            ]).ConfigureAwait(false);
    }


    public IBindingSpecification SourceExchange(string exchange)
    {
        ToQueue = false;
        Source = exchange;
        return this;
    }

    public IBindingSpecification DestinationQueue(string queue)
    {
        ToQueue = true;
        Destination = queue;
        return this;
    }

    public IBindingSpecification DestinationExchange(string exchange)
    {
        Destination = exchange;
        return this;
    }

    public IBindingSpecification Key(string key)
    {
        RoutingKey = key;
        return this;
    }

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
}

public class AmqpUnBindingSpecification(AmqpManagement management)
    : BindingSpecificationBase, IUnbindSpecification // TODO: Create a common class
{
    private AmqpManagement Management { get; } = management;


    public IUnbindSpecification SourceExchange(string exchange)
    {
        Source = exchange;
        return this;
    }

    public IUnbindSpecification DestinationQueue(string queue)
    {
        ToQueue = true;
        Destination = queue;
        return this;
    }

    public IUnbindSpecification DestinationExchange(string exchange)
    {
        ToQueue = false;
        Destination = exchange;
        return this;
    }

    public IUnbindSpecification Key(string key)
    {
        RoutingKey = key;
        return this;
    }

    public IUnbindSpecification Argument(string key, object value)
    {
        _arguments[key] = value;
        return this;
    }

    public IUnbindSpecification Arguments(Dictionary<string, object> arguments)
    {
        _arguments = arguments;
        return this;
    }


    private string BindingsTarget(
        string destinationField, string source, string destination, string key)
    {
        return "/bindings?src="
               + Utils.EncodePathSegment(source)
               + "&"
               + destinationField
               + "="
               + Utils.EncodePathSegment((destination))
               + "&key="
               + Utils.EncodePathSegment(key);
    }

    private async Task<List<Map>> GetBindings(string path)
    {
        var result = await Management.Request(
            null, path,
            AmqpManagement.Get, new[] { AmqpManagement.Code200 }).ConfigureAwait(false);

        if (result.Body is not List list)
        {
            return [];
        }

        if (list.Count > 0 && list[0] is Map map)
        {
            return [map];
        }


        return [];
    }

    // private static Optional<String> matchBinding(
    //     List<Map<String, Object>> bindings, String key, Map<String, Object> arguments) {
    //     Optional<String> uri;
    //     if (!bindings.isEmpty()) {
    //         uri =
    //             bindings.stream()
    //                 .filter(
    //                     binding -> {
    //             String bindingKey = (String) binding.get("binding_key");
    //             @SuppressWarnings("unchecked")
    //             Map<String, Object> bindingArguments =
    //                 (Map<String, Object>) binding.get("arguments");
    //             if (key == null && bindingKey == null
    //                 || key != null && key.equals(bindingKey)) {
    //                 return arguments == null && bindingArguments == null
    //                        || arguments != null && arguments.equals(bindingArguments);
    //             }
    //             return false;
    //         })
    //         .map(b -> b.get("location").toString())
    //             .findFirst();
    //     } else {
    //         uri = Optional.empty();
    //     }
    //     return uri;
    // }


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


    public async Task UnBind()
    {
        string destinationCharacter = ToQueue ? "dstq" : "dste";
        if (_arguments.Count == 0)
        {
            string target =
                $"/{Consts.Bindings}/src={Utils.EncodePathSegment(Source)};" +
                $"{($"{destinationCharacter}={Utils.EncodePathSegment(Destination)};" +
                    $"key={Utils.EncodePathSegment(RoutingKey)};args=")}";

            await Management.Request(
                null, target,
                AmqpManagement.Delete, new[] { AmqpManagement.Code204 }).ConfigureAwait(false);
        }
        else
        {
            string path = BindingsTarget(destinationCharacter, Source, Destination, RoutingKey);
            var bindings = await GetBindings(path).ConfigureAwait(false);
            string? uri = MatchBinding(bindings, RoutingKey, ArgsToMap());
            if (uri != null)
            {
                await Management.Request(
                    null, uri,
                    AmqpManagement.Delete, new[] { AmqpManagement.Code204 }).ConfigureAwait(false);
            }
        }
    }
}

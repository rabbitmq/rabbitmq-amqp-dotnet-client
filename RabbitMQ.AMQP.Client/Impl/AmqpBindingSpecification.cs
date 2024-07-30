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
        var kv = new Map
        {
            { "source", Source },
            { "binding_key", RoutingKey },
            { "arguments", ArgsToMap() },
            { ToQueue ? "destination_queue" : "destination_exchange", Destination }
        };

        await Management.RequestAsync(kv, $"/{Consts.Bindings}",
            AmqpManagement.Post,
            [
                AmqpManagement.Code204,
            ]).ConfigureAwait(false);
    }

    public async Task Unbind()
    {
        string destinationCharacter = ToQueue ? "dstq" : "dste";
        if (_arguments.Count == 0)
        {
            string target =
                $"/{Consts.Bindings}/src={Utils.EncodePathSegment(Source)};" +
                $"{($"{destinationCharacter}={Utils.EncodePathSegment(Destination)};" +
                    $"key={Utils.EncodePathSegment(RoutingKey)};args=")}";

            await Management.RequestAsync(
                null, target,
                AmqpManagement.Delete, new[] { AmqpManagement.Code204 }).ConfigureAwait(false);
        }
        else
        {
            string path = BindingsTarget(destinationCharacter, Source, Destination, RoutingKey);
            List<Map> bindings = await GetBindings(path).ConfigureAwait(false);
            string? uri = MatchBinding(bindings, RoutingKey, ArgsToMap());
            if (uri != null)
            {
                await Management.RequestAsync(
                    null, uri,
                    AmqpManagement.Delete, new[] { AmqpManagement.Code204 }).ConfigureAwait(false);
            }
        }
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

    private string BindingsTarget(
        string destinationField, string source, string destination, string key)
    {
        return "/bindings?src="
               + Utils.EncodeHttpParameter(source)
               + "&"
               + destinationField
               + "="
               + Utils.EncodeHttpParameter(destination)
               + "&key="
               + Utils.EncodeHttpParameter(key);
    }

    private async Task<List<Map>> GetBindings(string path)
    {
        Amqp.Message result = await Management.RequestAsync(
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

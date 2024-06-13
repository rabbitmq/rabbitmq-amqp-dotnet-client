using System.Security.Cryptography;
using System.Text;

namespace RabbitMQ.AMQP.Client.Impl;

public static class Utils
{
    private const string DefaultPrefix = "client.gen-";
    
    public static string GenerateQueueName()
    {
        return GenerateName(DefaultPrefix);
    }

    private static string GenerateName(string prefix)
    {
        var uuid = Guid.NewGuid().ToString();
        var digest = MD5.HashData(Encoding.UTF8.GetBytes(uuid));
        return prefix + Convert.ToBase64String(digest)
            .Replace('+', '-')
            .Replace('/', '_')
            .Replace("=", "");
    }


    public static RabbitMQ.AMQP.Client.Error? ConvertError(Amqp.Framing.Error? sourceError)
    {
        Error? resultError = null;

        if (sourceError != null)
        {
            resultError = new Error
            {
                Description = sourceError.Description,
                ErrorCode = sourceError.Condition.ToString()
            };
        }
        
        return resultError;

    }
        

    
}
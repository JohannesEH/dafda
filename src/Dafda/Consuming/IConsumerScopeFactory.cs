using Dafda.Configuration;

namespace Dafda.Consuming
{
    public interface IConsumerScopeFactory
    {
        ConsumerScope CreateConsumerScope();
    }
}
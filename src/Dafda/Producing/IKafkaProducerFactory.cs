using Dafda.Configuration;

namespace Dafda.Producing
{
    public interface IKafkaProducerFactory
    {
        IKafkaProducer CreateProducer(IProducerConfiguration configuration);
    }
}
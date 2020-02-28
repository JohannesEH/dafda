using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Dafda.Consuming;
using Dafda.Logging;
using Metadata = Confluent.Kafka.Metadata;

namespace Dafda.Producing
{
    internal class KafkaProducer : IDisposable
    {
        private static readonly ILog Log = LogProvider.GetCurrentClassLogger();
        private readonly IProducer<string, string> _innerKafkaProducer;

        public KafkaProducer(IEnumerable<KeyValuePair<string, string>> configuration)
        {
            _innerKafkaProducer = new ProducerBuilder<string, string>(configuration).Build();
        }

        [Obsolete]
        public virtual async Task Produce(OutgoingMessage outgoingMessage)
        {
            try
            {
                Log.Debug("Producing message {Type} with {Key} on {Topic}", outgoingMessage.Type, outgoingMessage.Key, outgoingMessage.Topic);

                var msg = PrepareOutgoingMessage(outgoingMessage);
                await _innerKafkaProducer.ProduceAsync(outgoingMessage.Topic, msg);

                Log.Debug("Message for {Type} with id {MessageId} was published", outgoingMessage.Type, outgoingMessage.MessageId);
            }
            catch (ProduceException<string, string> e)
            {
                Log.Error(e, "Error publishing message due to: {ErrorReason} ({ErrorCode})", e.Error.Reason, e.Error.Code);
                throw;
            }
        }

        public static Message<string, string> PrepareOutgoingMessage(OutgoingMessage outgoingMessage)
        {
            return new Message<string, string>
            {
                Key = outgoingMessage.Key,
                Value = outgoingMessage.Value,
            };
        }

        public IPayloadSerializer Serializer { get; set; } = new DefaultPayloadSerializer();

        public virtual async Task Produce(IncomingOutgoingMessage message)
        {
            try
            {
                await InternalProduce(
                    topic: message.TopicName,
                    key: message.PartitionKey,
                    value: SerializePayload(message.Payload)
                );
            }
            catch (ProduceException<string, string> e)
            {
                Log.Error(e, "Error publishing message due to: {ErrorReason} ({ErrorCode})", e.Error.Reason, e.Error.Code);
                throw;
            }
        }

        private string SerializePayload(object payload)
        {
            return Serializer?.Serialize(payload);
        }

        protected virtual Task InternalProduce(string topic, string key, string value)
        {
            var task = InternalProduce(
                topic: topic,
                innerKafkaMessage: new Message<string, string>
                {
                    Key = key,
                    Value = value
                }
            );

            return task;
        }

        protected virtual Task<DeliveryResult<string, string>> InternalProduce(string topic, Message<string, string> innerKafkaMessage)
        {
            return _innerKafkaProducer.ProduceAsync(topic, innerKafkaMessage);
        }

        public virtual void Dispose()
        {
            _innerKafkaProducer?.Dispose();
        }
    }

    public class IncomingOutgoingMessage
    {
        public IncomingOutgoingMessage(string topicName, string partitionKey, object payload)
        {
            TopicName = topicName;
            PartitionKey = partitionKey;
            Payload = payload;
        }

        public string TopicName { get; }
        public string PartitionKey { get; }
        public object Payload { get; }
    }

    public interface IPayloadSerializer
    {
        string Serialize(object payload);
    }

    public class DefaultPayloadSerializer : IPayloadSerializer
    {
        private readonly JsonSerializerOptions _jsonSerializerOptions;

        public DefaultPayloadSerializer()
        {
            _jsonSerializerOptions = new JsonSerializerOptions
            {
                IgnoreNullValues = false,
                DictionaryKeyPolicy = JsonNamingPolicy.CamelCase,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                
            };
        }

        public string Serialize(object payload)
        {
            return JsonSerializer.Serialize(payload, _jsonSerializerOptions);
        }
    }
}
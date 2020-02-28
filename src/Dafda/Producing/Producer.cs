using System.Collections.Generic;
using System.Threading.Tasks;
using Dafda.Outbox;

namespace Dafda.Producing
{
    public sealed class Producer
    {
        private readonly KafkaProducer _kafkaProducer;
        private readonly OutgoingMessageFactory _outgoingMessageFactory;

        internal Producer(KafkaProducer kafkaProducer, OutgoingMessageRegistry outgoingMessageRegistry, MessageIdGenerator messageIdGenerator)
        {
            _kafkaProducer = kafkaProducer;
            _outgoingMessageFactory = new OutgoingMessageFactory(outgoingMessageRegistry, messageIdGenerator);
        }

        internal string Name { get; set; } = "__Default Producer__";
        
        public async Task Produce(object message)
        {
            var outgoingMessage = AssembleOutgoingMessage(message);
            await _kafkaProducer.Produce(outgoingMessage);
        }

        public IPayloadComposer PayloadComposer { get; set; } = new DefaultPayloadComposer();

        public async Task Produce(object message, Dictionary<string, object> headers)
        {
            var outgoingMessage = AssembleOutgoingMessage(message);

            var payloadDescriptor = new PayloadDescriptor(
                outgoingMessage.MessageId,
                outgoingMessage.Topic,
                outgoingMessage.Key,
                outgoingMessage.Type,
                message,
                headers
            );

            await _kafkaProducer.Produce(new IncomingOutgoingMessage(
                    outgoingMessage.Topic,
                    outgoingMessage.Key,
                    await PayloadComposer.ComposeFrom(payloadDescriptor)
                ));
        }

        private OutgoingMessage AssembleOutgoingMessage(object message)
        {
            if (message is OutboxMessage outboxMessage)
            {
                return new OutgoingMessageBuilder()
                    .WithTopic(outboxMessage.Topic)
                    .WithMessageId(outboxMessage.MessageId.ToString())
                    .WithKey(outboxMessage.Key)
                    .WithValue(outboxMessage.Data)
                    .WithType(outboxMessage.Type)
                    .Build();
            }

            return _outgoingMessageFactory.Create(message);
        }
    }

    public interface IPayloadComposer
    {
        Task<object> ComposeFrom(PayloadDescriptor descriptor);
    }

    public class DefaultPayloadComposer : IPayloadComposer
    {
        public Task<object> ComposeFrom(PayloadDescriptor descriptor)
        {
            var envelope = new Dictionary<string, object>
            {
                { "MessageId", descriptor.MessageId },
                { "Type", descriptor.MessageType },
                { "Data", descriptor.MessageData },
            };

            return Task.FromResult((object)envelope);
        }
    }

    public sealed class PayloadDescriptor
    {
        public PayloadDescriptor(string messageId, string topicName, string partitionKey, string messageType, object messageData, IEnumerable<KeyValuePair<string, object>> messageHeaders)
        {
            MessageId = messageId;
            TopicName = topicName;
            PartitionKey = partitionKey;
            MessageType = messageType;
            MessageData = messageData;
            MessageHeaders = messageHeaders;
        }

        public string TopicName { get; private set; }
        public string PartitionKey { get; private set; }
        public string MessageId { get; private set; }
        public string MessageType { get; private set; }
        public IEnumerable<KeyValuePair<string, object>> MessageHeaders { get; private set; }
        public object MessageData { get; private set; }
    }
}
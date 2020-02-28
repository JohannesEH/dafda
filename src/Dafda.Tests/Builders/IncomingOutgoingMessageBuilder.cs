using Dafda.Producing;

namespace Dafda.Tests.Builders
{
    public class IncomingOutgoingMessageBuilder
    {
        private string _topicName;
        private string _partitionKey;
        private object _payload;

        public IncomingOutgoingMessageBuilder()
        {
            _topicName = "foo-topic-name";
            _partitionKey = "foo-partition-key";
            _payload = "foo-payload";
        }

        public IncomingOutgoingMessageBuilder WithTopicName(string topicName)
        {
            _topicName = topicName;
            return this;
        }

        public IncomingOutgoingMessageBuilder WithPartitionKey(string partitionKey)
        {
            _partitionKey = partitionKey;
            return this;
        }

        public IncomingOutgoingMessageBuilder WithPayload(object payload)
        {
            _payload = payload;
            return this;
        }

        public IncomingOutgoingMessage Build()
        {
            return new IncomingOutgoingMessage(
                topicName: _topicName,
                partitionKey: _partitionKey,
                payload: _payload
            );
        }
    }
}
using System.Threading.Tasks;
using Dafda.Producing;
using Dafda.Tests.Builders;
using Dafda.Tests.TestDoubles;
using Xunit;

namespace Dafda.Tests.Producing.Kafka
{
    public class TestKafkaProducer
    {
        private static OutgoingMessageBuilder EmptyOutgoingMessage =>
            new OutgoingMessageBuilder()
                .WithTopic("")
                .WithMessageId("")
                .WithKey("")
                .WithValue("")
                .WithType("");

        [Fact]
        public void Message_has_expected_key()
        {
            var message = KafkaProducer.PrepareOutgoingMessage(EmptyOutgoingMessage.WithKey("dummyKey"));

            Assert.Equal("dummyKey", message.Key);
        }

        [Fact]
        public void Message_has_expected_value()
        {
            var message = KafkaProducer.PrepareOutgoingMessage(EmptyOutgoingMessage.WithValue("dummyMessage"));

            Assert.Equal("dummyMessage", message.Value);
        }

        [Fact]
        public async Task produces_to_expected_topic()
        {
            var spy = new KafkaProducerSpy();

            var stub = new IncomingOutgoingMessageBuilder().Build();
            await spy.Produce(stub);

            Assert.Equal(stub.TopicName, spy.Topic);
        }

        [Fact]
        public async Task produces_message_with_expected_key()
        {
            var spy = new KafkaProducerSpy();

            var expected = "foo partition key";
            
            var stub = new IncomingOutgoingMessageBuilder()
                .WithPartitionKey(expected)
                .Build();
            
            await spy.Produce(stub);

            Assert.Equal(expected, spy.Key);
        }

        [Fact]
        public async Task produces_message_with_expected_value()
        {
            var expected = "foo value";

            var spy = new KafkaProducerSpy();
            spy.Serializer = new PayloadSerializerStub(expected);

            var stub = new IncomingOutgoingMessageBuilder().Build();
            
            await spy.Produce(stub);

            Assert.Equal(expected, spy.Value);
        }
    }
}
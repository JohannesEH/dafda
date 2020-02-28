using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Dafda.Producing;
using Dafda.Tests.TestDoubles;
using Xunit;

namespace Dafda.Tests.Producing
{
    public class TestProducer
    {
        [Fact]
        public async Task Can_produce_message()
        {
            var spy = new KafkaProducerSpy();

            var sut = A.Producer
                .With(spy)
                .With(A.OutgoingMessageRegistry
                    .Register<Message>("foo", "bar", @event => @event.Id)
                    .Build()
                )
                .Build();

            await sut.Produce(new Message {Id = "dummyId"});

            Assert.Equal("foo", spy.LastMessage.Topic);
            Assert.Equal("dummyId", spy.LastMessage.Key);
        }

        public class Message
        {
            public string Id { get; set; }
        }

        [Fact]
        public async Task produces_message_to_expected_topic()
        {
            var spy = new KafkaProducerSpy();

            var expectedTopic = "foo";

            var sut = A.Producer
                .With(spy)
                .With(A.OutgoingMessageRegistry
                    .Register<Message>(expectedTopic, "bar", @event => @event.Id)
                    .Build()
                )
                .Build();

            await sut.Produce(
                message: new Message { Id = "dummyId" },
                headers: new Dictionary<string, object>
                {
                    {"foo-key", "foo-value"}
                }
            );

            Assert.Equal(expectedTopic, spy.Topic);
        }

        [Fact]
        public async Task produces_message_with_expected_partition_key()
        {
            var spy = new KafkaProducerSpy();

            var expectedKey = "foo-partition-key";

            var sut = A.Producer
                .With(spy)
                .With(A.OutgoingMessageRegistry
                    .Register<Message>("foo", "bar", @event => @event.Id)
                    .Build()
                )
                .Build();

            await sut.Produce(
                message: new Message { Id = expectedKey },
                headers: new Dictionary<string, object>
                {
                    {"foo-key", "foo-value"}
                }
            );

            Assert.Equal(expectedKey, spy.Key);
        }

        [Fact]
        public async Task produces_message_with_expected_value()
        {
            var expectedValue = "foo-value";
            
            var spy = new KafkaProducerSpy();
            spy.Serializer = new PayloadSerializerStub(expectedValue);

            var sut = A.Producer
                .With(spy)
                .With(A.OutgoingMessageRegistry
                    .Register<Message>("foo", "bar", @event => @event.Id)
                    .Build()
                )
                .Build();

            await sut.Produce(
                message: new Message { Id = "dummyId" },
                headers: new Dictionary<string, object>
                {
                    {"foo-key", "foo-value"}
                }
            );

            Assert.Equal(expectedValue, spy.Value);
        }

        [Fact]
        public async Task produces_message_with_expected_value_2()
        {
            var spy = new KafkaProducerSpy();

            var sut = A.Producer
                .With(spy)
                .With(new MessageIdGeneratorStub(() => "1"))
                .With(A.OutgoingMessageRegistry
                    .Register<Message>("foo", "bar", @event => @event.Id)
                    .Build()
                )
                .Build();

            await sut.Produce(
                message: new Message { Id = "dummyId" },
                headers: new Dictionary<string, object>
                {
                    {"foo-key", "foo-value"}
                }
            );

            var expectedValue = "{\"messageId\":\"1\",\"type\":\"bar\",\"data\":{\"id\":\"dummyId\"}}";

            Assert.Equal(expectedValue, spy.Value);
        }
    }

    public class TestDefaultPayloadSerializer
    {
        [Fact]
        public void duno()
        {
            var sut = new DefaultPayloadSerializer();
            var result = sut.Serialize(new
            {
                Foo = "bar"
            });

            var expected = "{\"foo\":\"bar\"}";

            Assert.Equal(expected, result);
        }

        [Fact]
        public void duno2()
        {
            var sut = new DefaultPayloadSerializer();
            var result = sut.Serialize(new
            {
                Foo = "foo-value",
                Bar = new { Baz = "baz-value" }
            });

            var expected = "{\"foo\":\"foo-value\",\"bar\":{\"baz\":\"baz-value\"}}";

            Assert.Equal(expected, result);
        }

        [Fact]
        public void duno3()
        {
            var sut = new DefaultPayloadSerializer();
            var result = sut.Serialize(new
            {
                Foo = "bar",
                Baz =  new Dictionary<string, string>
                {
                    { "Qux", "qux-value" }
                }
            });

            var expected = "{\"foo\":\"bar\",\"baz\":{\"qux\":\"qux-value\"}}";

            Assert.Equal(expected, result);
        }

        [Fact]
        public void duno4()
        {
            var sut = new DefaultPayloadSerializer();
            var result = sut.Serialize(new
            {
                Foo = "bar",
                Baz =  new Dictionary<string, object>
                {
                    { "Qux", "qux-value" },
                    { "Item", new { Value = "value" } }
                }
            });

            var expected = "{\"foo\":\"bar\",\"baz\":{\"qux\":\"qux-value\",\"item\":{\"value\":\"value\"}}}";

            Assert.Equal(expected, result);
        }
    }

}
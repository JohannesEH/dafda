using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Dafda.Producing;

namespace Dafda.Tests.TestDoubles
{
    internal class KafkaProducerSpy : KafkaProducer
    {
        public KafkaProducerSpy() : this(Enumerable.Empty<KeyValuePair<string, string>>())
        {
            
        }

        public KafkaProducerSpy(IEnumerable<KeyValuePair<string, string>> configuration) : base(configuration)
        {

        }

        public override Task Produce(OutgoingMessage outgoingMessage)
        {
            LastMessage = outgoingMessage;
            return Task.CompletedTask;
        }

        protected override Task InternalProduce(string topic, string key, string value)
        {
            Topic = topic;
            Key = key;
            Value = value;

            return Task.CompletedTask;
        }

        public override void Dispose()
        {
            base.Dispose();
            WasDisposed = true;
        }

        [Obsolete]
        public OutgoingMessage LastMessage { get; private set; }

        public bool WasDisposed { get; private set; }

        public string Topic { get; private set; }
        public string Key { get; private set; }
        public string Value { get; private set; }
    }
}
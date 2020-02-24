using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dafda.Producing;

namespace Dafda.Outbox
{
    /// <summary></summary>
    public sealed class OutboxQueue
    {
        private readonly IOutboxMessageRepository _repository;
        private readonly IOutboxNotifier _outboxNotifier;
        private readonly OutgoingMessageFactory _outgoingMessageFactory;

        internal OutboxQueue(MessageIdGenerator messageIdGenerator, OutgoingMessageRegistry outgoingMessageRegistry, IOutboxMessageRepository repository, IOutboxNotifier outboxNotifier)
        {
            _repository = repository;
            _outboxNotifier = outboxNotifier;
            _outgoingMessageFactory = new OutgoingMessageFactory(outgoingMessageRegistry, messageIdGenerator);
        }

        /// <summary>
        /// Send domain events to be processed by the Dafda outbox feature
        /// </summary>
        /// <param name="events">The list of domain events</param>
        /// <returns>
        /// A <see cref="IOutboxNotifier"/> which can be used to signal the outbox processing mechanism,
        /// whether local or remote. The <see cref="IOutboxNotifier.Notify"/> can be used to signal the
        /// processor when new events are available.
        /// </returns>
        /// <remarks>
        /// Calling <see cref="IOutboxNotifier.Notify"/> can happen as part of a transaction, e.g. when
        /// using Postgres' <c>LISTEN/NOTIFY</c>, or after the transactions has been committed, when using
        /// the built-in <see cref="IOutboxNotifier"/>.
        /// </remarks>
        public async Task<IOutboxNotifier> Enqueue(IEnumerable<object> events)
        {
            var outboxMessages = events
                .Select(CreateOutboxMessage)
                .ToArray();

            await _repository.Add(outboxMessages);

            return _outboxNotifier;
        }

        private OutboxMessage CreateOutboxMessage(object @event)
        {
            var outgoingMessage = _outgoingMessageFactory.Create(@event);

            var messageId = outgoingMessage.MessageId;
            var correlationId = Guid.NewGuid().ToString();
            var topic = outgoingMessage.Topic;
            var key = outgoingMessage.Key;
            var type = outgoingMessage.Type;
            var format = "application/json";
            var data = outgoingMessage.Value;

            return new OutboxMessage(Guid.Parse(messageId), correlationId, topic, key, type, format, data, DateTime.UtcNow);
        }
    }
}
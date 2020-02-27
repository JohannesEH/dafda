using System;
using System.Threading;
using System.Threading.Tasks;
using Dafda.Logging;
using Dafda.Producing;

namespace Dafda.Outbox
{
    internal class OutboxDispatcher
    {
        private static readonly ILog Log = LogProvider.GetCurrentClassLogger();

        private readonly IOutboxUnitOfWorkFactory _unitOfWorkFactory;
        private readonly Producer _producer;

        public OutboxDispatcher(IOutboxUnitOfWorkFactory unitOfWorkFactory, Producer producer)
        {
            _unitOfWorkFactory = unitOfWorkFactory;
            _producer = producer;
        }

        public async Task Dispatch(CancellationToken cancellationToken)
        {
            using (var outboxUnitOfWork = _unitOfWorkFactory.Begin())
            {
                var outboxMessages = await outboxUnitOfWork.GetAllUnpublishedMessages(cancellationToken);

                Log.Debug("Unpublished outbox messages: {OutboxMessageCount}", outboxMessages.Count);

                try
                {
                    foreach (var outboxMessage in outboxMessages)
                    {
                        await _producer.Produce(outboxMessage);

                        outboxMessage.MaskAsProcessed();

                        Log.Debug(@"Published outbox message {MessageId} ({Type})", outboxMessage.MessageId, outboxMessage.Type);
                    }
                }
                catch (Exception exception)
                {
                    Log.Error("Error while publishing outbox messages", exception);
                    throw;
                }

                await outboxUnitOfWork.Commit(cancellationToken);
            }
        }
    }
}
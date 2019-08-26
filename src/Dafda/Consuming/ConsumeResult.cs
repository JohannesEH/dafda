using System;
using System.Threading.Tasks;
using Dafda.Messaging;

namespace Dafda.Consuming
{
    public class ConsumeResult
    {
        private static readonly Func<Task> EmptyCommitAction = () => Task.CompletedTask;
        private readonly Func<Task> _onCommit;

        public ConsumeResult(ITransportLevelMessage message, Func<Task> onCommit = null)
        {
            Message = message;
            _onCommit = onCommit ?? EmptyCommitAction;
        }

        public ITransportLevelMessage Message { get; }

        public async Task Commit()
        {
            await _onCommit();
        }
    }
}
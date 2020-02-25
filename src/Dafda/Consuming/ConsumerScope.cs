using System;
using System.Threading;
using System.Threading.Tasks;

namespace Dafda.Consuming
{
    internal abstract class ConsumerScope : IDisposable
    {
        public abstract Task<MessageResult> GetNext(CancellationToken cancellationToken);
        public abstract void Dispose();
    }
}
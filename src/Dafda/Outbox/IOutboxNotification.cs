using System.Threading;
using System.Threading.Tasks;

namespace Dafda.Outbox
{
    public interface IOutboxNotification : IOutboxNotifier
    {
        Task<bool> Wait(CancellationToken cancellationToken);
    }
}
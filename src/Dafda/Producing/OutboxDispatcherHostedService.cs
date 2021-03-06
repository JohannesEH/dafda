using System;
using System.Threading;
using System.Threading.Tasks;
using Dafda.Outbox;
using Microsoft.Extensions.Hosting;

namespace Dafda.Producing
{
    internal class OutboxDispatcherHostedService : IHostedService, IDisposable
    {
        private readonly IOutboxNotification _outboxNotification;
        private readonly OutboxDispatcher _outboxDispatcher;
        private Thread _thread;
        private CancellationTokenSource _cancellationTokenSource;

        public OutboxDispatcherHostedService(IOutboxUnitOfWorkFactory unitOfWorkFactory, OutboxProducer producer, IOutboxNotification outboxNotification)
        {
            _outboxNotification = outboxNotification;
            _outboxDispatcher = new OutboxDispatcher(unitOfWorkFactory, producer);
        }

        private void ThreadProc()
        {
            try
            {
                ProcessOutbox(_cancellationTokenSource.Token);
            }
            catch (ThreadAbortException)
            {
            }
        }

        public void ProcessOutbox(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                _outboxDispatcher.Dispatch(cancellationToken).Wait(cancellationToken);
                _outboxNotification.Wait();
            }
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _cancellationTokenSource = new CancellationTokenSource();

            _thread = new Thread(ThreadProc);
            _thread.IsBackground = true;

            _thread.Start();

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Stop();
            return Task.CompletedTask;
        }

        private void Stop()
        {
            _cancellationTokenSource?.Cancel();

            _outboxNotification.Notify();
            _thread?.Join();
            _thread = null;
        }

        public void Dispose()
        {
            Stop();
            _cancellationTokenSource?.Dispose();
        }
    }
}
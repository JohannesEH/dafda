using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Dafda.Consuming
{
    internal class ConsumerHostedService : BackgroundService
    {
        private readonly ILogger<ConsumerHostedService> _logger;
        private readonly IApplicationLifetime _applicationLifetime;
        private readonly Consumer _consumer;
        private readonly string _groupId;

        public ConsumerHostedService(ILogger<ConsumerHostedService> logger, IApplicationLifetime applicationLifetime, Consumer consumer, string groupId)
        {
            _logger = logger;
            _applicationLifetime = applicationLifetime;
            _consumer = consumer;
            _groupId = groupId;
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(async () =>
            {
                try
                {
                    _logger.LogDebug("ConsumerHostedService [{GroupId}] started", _groupId);
                    await _consumer.ConsumeAll(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogDebug("ConsumerHostedService [{GroupId}] cancelled", _groupId);
                }
                catch (Exception err)
                {
                    _logger.LogError(err, "Unhandled error occurred while consuming messaging");
                    _applicationLifetime.StopApplication();
                }
            }, stoppingToken);
        }
    }
}
using System;
using Dafda.Outbox;
using Dafda.Producing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Dafda.Configuration
{
    public static class OutboxProducerServiceCollectionExtensions
    {
        public static void AddOutbox(this IServiceCollection services, Action<OutboxProducerOptions> options)
        {
            var outgoingMessageRegistry = new OutgoingMessageRegistry();
            var configurationBuilder = new ProducerConfigurationBuilder();
            var outboxProducerOptions = new OutboxProducerOptions(configurationBuilder, services, outgoingMessageRegistry);
            options?.Invoke(outboxProducerOptions);
            var producerConfiguration = configurationBuilder.Build();

            services.AddSingleton<IOutboxWaiter, OutboxWaiter>(provider => new OutboxWaiter(outboxProducerOptions.DispatchInterval));

            services.AddTransient<OutboxQueue>(provider =>
            {
                var messageIdGenerator = producerConfiguration.MessageIdGenerator;
                var outboxMessageRepository = provider.GetRequiredService<IOutboxMessageRepository>();
                return new OutboxQueue(messageIdGenerator, outgoingMessageRegistry, outboxMessageRepository);
            });

            services.AddTransient<IHostedService, OutboxDispatcherHostedService>(provider =>
            {
                var messageIdGenerator = producerConfiguration.MessageIdGenerator;
                var kafkaProducer = producerConfiguration.KafkaProducerFactory();

                var producer = new Producer(kafkaProducer, outgoingMessageRegistry, messageIdGenerator);

                return new OutboxDispatcherHostedService(
                    unitOfWorkFactory: provider.GetRequiredService<IOutboxUnitOfWorkFactory>(),
                    producer: producer,
                    outboxWaiter: provider.GetRequiredService<IOutboxWaiter>()
                );
            });
        }
    }
}
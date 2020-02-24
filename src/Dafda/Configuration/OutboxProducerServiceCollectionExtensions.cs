using System;
using Dafda.Outbox;
using Dafda.Producing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Dafda.Configuration
{
    /// <summary></summary>
    public static class OutboxProducerServiceCollectionExtensions
    {
        /// <summary>
        /// Add the Dafda implementation of the Outbox pattern. The publisher runs in a
        /// <see cref="IHostedService"/>. 
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection"/> used in <c>Startup</c>.</param>
        /// <param name="options">Use this action to override Dafda and underlying Kafka configuration.</param>
        public static void AddOutbox(this IServiceCollection services, Action<OutboxProducerOptions> options)
        {
            var outgoingMessageRegistry = new OutgoingMessageRegistry();
            var configurationBuilder = new ProducerConfigurationBuilder();
            var outboxProducerOptions = new OutboxProducerOptions(configurationBuilder, services, outgoingMessageRegistry);
            options?.Invoke(outboxProducerOptions);
            var producerConfiguration = configurationBuilder.Build();

            var outboxWaiter = new OutboxNotification(outboxProducerOptions.DispatchInterval);

            services.AddTransient<OutboxQueue>(provider =>
            {
                var messageIdGenerator = producerConfiguration.MessageIdGenerator;
                var outboxMessageRepository = provider.GetRequiredService<IOutboxMessageRepository>();
                return new OutboxQueue(messageIdGenerator, outgoingMessageRegistry, outboxMessageRepository, outboxWaiter);
            });

            services.AddTransient<IHostedService, OutboxDispatcherHostedService>(provider =>
            {
                var messageIdGenerator = producerConfiguration.MessageIdGenerator;
                var kafkaProducer = producerConfiguration.KafkaProducerFactory();

                var producer = new Producer(kafkaProducer, outgoingMessageRegistry, messageIdGenerator);

                return new OutboxDispatcherHostedService(
                    unitOfWorkFactory: provider.GetRequiredService<IOutboxUnitOfWorkFactory>(),
                    producer: producer,
                    outboxNotification: outboxWaiter
                );
            });
        }
    }
}
using Bogus;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;

namespace Demo
{
    class Program
    {
        static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        private static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, collection) =>
                {
                    collection.AddHostedService<KafkaProducerHostedService>();
                });
    }

    public class KafkaProducerHostedService : IHostedService
    {
        private readonly ILogger<KafkaProducerHostedService> _logger;
        private IProducer<Null, string> _producer;

        public KafkaProducerHostedService(ILogger<KafkaProducerHostedService> logger)
        {
            _logger = logger;
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092"
            };
            _producer = new ProducerBuilder<Null, string>(config).Build();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            var faker = new Faker<Info>()
                .RuleFor(a => a.Name, f => f.Person.FullName)
                .RuleFor(a => a.Product, f => f.Commerce.ProductName())
                .RuleFor(a => a.Time, f => (decimal?)f.Random.Decimal(1, 30));           
            
            do
            {
                var value = JsonConvert.SerializeObject(faker.Generate());

                _logger.LogInformation($"Message generated at {DateTime.Now}: {value}");
                await _producer.ProduceAsync("demo", new Message<Null, string>()
                {
                    Value = value
                }, cancellationToken);

                var eventMessage = $"The user pressed the key {Console.ReadKey(true).Key}";
                _logger.LogInformation($"Message generated at {DateTime.Now}: {eventMessage}");
                await _producer.ProduceAsync("events", new Message<Null, string>()
                {
                    Value = eventMessage
                }, cancellationToken);
                _producer.Flush(TimeSpan.FromSeconds(2));
                

            } while (Console.ReadKey(true).Key != ConsoleKey.Escape);            
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();
            return Task.CompletedTask;
        }
    }
}
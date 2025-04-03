using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Configurations;
using SqsToS3ServiceDemo;
using Xunit;

namespace SqsToS3ServiceDemo.Tests
{
    public class SqsToS3MultiQueueIntegrationTests : IAsyncLifetime
    {
        private readonly LocalStackTestcontainer _localStack;
        private IAmazonSQS _sqsClient = null!;
        private IAmazonS3 _s3Client = null!;
        private readonly Dictionary<string, string> _queueToBucket = new();

        public SqsToS3MultiQueueIntegrationTests()
        {
            _localStack = new TestcontainersBuilder<LocalStackTestcontainer>()
                .WithImage("localstack/localstack:latest")
                .WithName("localstack-main")
                .WithPortBinding(4566, 4566)
                .WithEnvironment("DEBUG", "0")
                .WithBindMount("/var/run/docker.sock", "/var/run/docker.sock")
                .WithBindMount(Path.GetFullPath("./volume"), "/var/lib/localstack")
                .WithLocalStack(new LocalStackTestcontainerConfiguration
                {
                    Services = { LocalStackService.S3, LocalStackService.SQS }
                })
                .Build();
        }

        public async Task InitializeAsync()
        {
            await _localStack.StartAsync();

            var credentials = new BasicAWSCredentials("test", "test");
            var serviceUrl = _localStack.GetUrl();

            _sqsClient = new AmazonSQSClient(credentials, new AmazonSQSConfig
            {
                ServiceURL = serviceUrl,
                AuthenticationRegion = "us-east-1"
            });

            _s3Client = new AmazonS3Client(credentials, new AmazonS3Config
            {
                ServiceURL = serviceUrl,
                AuthenticationRegion = "us-east-1",
                ForcePathStyle = true
            });

            for (int i = 0; i < 2; i++)
            {
                var bucketName = $"test-bucket-{i}-{Guid.NewGuid()}";
                var queueName = $"test-queue-{i}-{Guid.NewGuid()}";

                await _s3Client.PutBucketAsync(bucketName);
                var queueResp = await _sqsClient.CreateQueueAsync(queueName);

                _queueToBucket[queueResp.QueueUrl] = bucketName;
            }
        }

        public async Task DisposeAsync()
        {
            _sqsClient.Dispose();
            _s3Client.Dispose();
            await _localStack.DisposeAsync();
        }

        [Fact]
        public async Task PollQueuesOnceAsync_Processes_AllQueues()
        {
            var service = new SqsToS3Service(_sqsClient, _s3Client);

            foreach (var (queueUrl, _) in _queueToBucket)
            {
                await _sqsClient.SendMessageAsync(queueUrl, $"{{ "source": "{ queueUrl}
                " }}");
            }

            int processed = await service.PollQueuesOnceAsync(_queueToBucket);

            Assert.Equal(_queueToBucket.Count, processed);

            foreach (var bucket in _queueToBucket.Values)
            {
                var result = await _s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = bucket });
                Assert.Single(result.S3Objects);
            }
        }

        [Fact]
        public async Task PollQueuesWithTimerAsync_Processes_Messages_AndStops()
        {
            var service = new SqsToS3Service(_sqsClient, _s3Client);
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(6));

            foreach (var (queueUrl, _) in _queueToBucket)
            {
                await _sqsClient.SendMessageAsync(queueUrl, $"{{ "early": "true" }}");
            }

            _ = service.PollQueuesWithTimerAsync(_queueToBucket, TimeSpan.FromSeconds(2), cts.Token);

            await Task.Delay(2500);

            foreach (var (queueUrl, _) in _queueToBucket)
            {
                await _sqsClient.SendMessageAsync(queueUrl, $"{{ "late": "true" }}");
            }

            await Task.Delay(5000);

            foreach (var bucket in _queueToBucket.Values)
            {
                var result = await _s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = bucket });
                Assert.True(result.S3Objects.Count >= 2);
            }
        }
    }
}
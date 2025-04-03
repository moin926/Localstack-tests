using System;
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
    public class SqsToS3IntegrationTests : IAsyncLifetime
    {
        private readonly LocalStackTestcontainer _localStack;
        private IAmazonSQS _sqsClient = null!;
        private IAmazonS3 _s3Client = null!;
        private string _bucketName = null!;
        private string _queueName = null!;
        private string _queueUrl = null!;

        public SqsToS3IntegrationTests()
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

            // Create bucket and queue
            _bucketName = $"bucket-{Guid.NewGuid()}";
            _queueName = $"queue-{Guid.NewGuid()}";

            await _s3Client.PutBucketAsync(_bucketName);

            var queueResponse = await _sqsClient.CreateQueueAsync(_queueName);
            _queueUrl = queueResponse.QueueUrl;
        }

        public async Task DisposeAsync()
        {
            _sqsClient.Dispose();
            _s3Client.Dispose();
            await _localStack.DisposeAsync();
        }

        [Fact]
        public async Task PollOnceAsync_Uploads_AllMessagesToS3()
        {
            // Arrange
            var service = new SqsToS3Service(_sqsClient, _s3Client);
            var msg1 = "{ \"type\": \"order.created\", \"id\": 1 }";
            var msg2 = "{ \"type\": \"order.shipped\", \"id\": 2 }";

            await _sqsClient.SendMessageAsync(_queueUrl, msg1);
            await _sqsClient.SendMessageAsync(_queueUrl, msg2);

            // Act
            int processed = await service.PollOnceAsync(_queueUrl, _bucketName);

            // Assert
            Assert.Equal(2, processed);
            var objects = await _s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = _bucketName });
            Assert.Equal(2, objects.S3Objects.Count);
        }

        [Fact]
        public async Task PollContinuouslyAsync_Processes_MessagesOverTime()
        {
            // Arrange
            var service = new SqsToS3Service(_sqsClient, _s3Client);
            var cts = new CancellationTokenSource();
            var pollTask = service.PollContinuouslyAsync(_queueUrl, _bucketName, TimeSpan.FromSeconds(2), cts.Token);

            // Send messages at different intervals
            await _sqsClient.SendMessageAsync(_queueUrl, "{ \"event\": \"alpha\" }");
            await Task.Delay(3000); // let the first message be processed
            await _sqsClient.SendMessageAsync(_queueUrl, "{ \"event\": \"beta\" }");
            await Task.Delay(3000); // let the second message be processed

            // Stop polling
            cts.Cancel();
            await pollTask;

            // Assert both were uploaded
            var list = await _s3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = _bucketName });
            Assert.Equal(2, list.S3Objects.Count);
        }

        [Fact]
        public async Task PollContinuouslyAsync_StopsOnCancel()
        {
            // Arrange
            var service = new SqsToS3Service(_sqsClient, _s3Client);
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromSeconds(5)); // auto-cancel

            // Act
            await service.PollContinuouslyAsync(_queueUrl, _bucketName, TimeSpan.FromSeconds(1), cts.Token);

            // Assert: no exceptions, method exits cleanly (test passes)
        }
    }
}

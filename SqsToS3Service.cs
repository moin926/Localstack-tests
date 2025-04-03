using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace SqsToS3ServiceDemo
{
    public class SqsToS3Service
    {
        private readonly IAmazonSQS _sqsClient;
        private readonly IAmazonS3 _s3Client;

        public SqsToS3Service(IAmazonSQS sqsClient, IAmazonS3 s3Client)
        {
            _sqsClient = sqsClient;
            _s3Client = s3Client;
        }

        /// <summary>
        /// Polls a single SQS queue once using long polling and uploads all received messages to S3.
        /// Each message is saved in the target bucket using its MessageId as the key.
        /// </summary>
        /// <param name="queueUrl">The URL of the SQS queue.</param>
        /// <param name="bucketName">The name of the target S3 bucket.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>The number of messages processed.</returns>
        public async Task<int> PollOnceAsync(string queueUrl, string bucketName, CancellationToken cancellationToken = default)
        {
            var receiveRequest = new ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                MaxNumberOfMessages = 10,
                WaitTimeSeconds = 10
            };

            var response = await _sqsClient.ReceiveMessageAsync(receiveRequest, cancellationToken);
            if (response.Messages == null || response.Messages.Count == 0)
                return 0;

            foreach (var message in response.Messages)
            {
                var putRequest = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = $"{message.MessageId}.json",
                    ContentBody = message.Body
                };

                await _s3Client.PutObjectAsync(putRequest, cancellationToken);
                await _sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle, cancellationToken);
            }

            return response.Messages.Count;
        }

        /// <summary>
        /// Polls multiple SQS queues once by iterating over a map of queue URLs to S3 bucket names.
        /// </summary>
        /// <param name="queueToBucketMap">Dictionary of queue URLs and corresponding bucket names.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>Total number of messages processed across all queues.</returns>
        public async Task<int> PollQueuesOnceAsync(
            Dictionary<string, string> queueToBucketMap,
            CancellationToken cancellationToken = default)
        {
            int totalProcessed = 0;

            foreach (var kvp in queueToBucketMap)
            {
                var processed = await PollOnceAsync(kvp.Key, kvp.Value, cancellationToken);
                totalProcessed += processed;
            }

            return totalProcessed;
        }

        /// <summary>
        /// Starts a background timer that continuously polls all queues in the given map
        /// at the specified interval until cancellation is requested.
        /// </summary>
        /// <param name="queueToBucketMap">Dictionary of queue URLs and target S3 buckets.</param>
        /// <param name="interval">Polling interval (e.g., every 5 seconds).</param>
        /// <param name="cancellationToken">Token to stop the polling loop.</param>
        /// <returns>A Task that completes when polling stops due to cancellation.</returns>
        public Task PollQueuesWithTimerAsync(
            Dictionary<string, string> queueToBucketMap,
            TimeSpan interval,
            CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource();
            Timer? timer = null;

            timer = new Timer(async _ =>
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    timer?.Dispose();
                    tcs.TrySetResult();
                    return;
                }

                try
                {
                    await PollQueuesOnceAsync(queueToBucketMap, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    timer?.Dispose();
                    tcs.TrySetResult();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Polling error: {ex.Message}");
                }
            }, null, TimeSpan.Zero, interval);

            cancellationToken.Register(() =>
            {
                timer?.Dispose();
                tcs.TrySetResult();
            });

            return tcs.Task;
        }
    }
}
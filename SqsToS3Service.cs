using System;
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
        /// Polls SQS once using long polling and uploads messages to S3.
        /// </summary>
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
        /// Continuously polls SQS at regular intervals until cancelled.
        /// </summary>
        /// <param name="queueUrl">URL of the SQS queue.</param>
        /// <param name="bucketName">S3 bucket name.</param>
        /// <param name="pollInterval">Delay between polls (e.g., 5 seconds).</param>
        /// <param name="cancellationToken">Used to stop the loop.</param>
        public async Task PollContinuouslyAsync(string queueUrl, string bucketName, TimeSpan pollInterval, CancellationToken cancellationToken)
        {
            Console.WriteLine($"Started polling every {pollInterval.TotalSeconds} seconds...");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var processed = await PollOnceAsync(queueUrl, bucketName, cancellationToken);

                    Console.WriteLine($"[{DateTime.UtcNow}] Processed {processed} message(s).");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error during polling: {ex.Message}");
                }

                try
                {
                    await Task.Delay(pollInterval, cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    break; // graceful shutdown
                }
            }

            Console.WriteLine("Polling stopped.");
        }
    }
}

using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;

using System;

/// <summary>
/// Service responsible for polling SQS queues and writing messages to S3.
/// </summary>
public class SqsToS3Service
{
    /// <summary>
    /// Amazon SQS client used for receiving and deleting messages.
    /// </summary>
    private readonly IAmazonSQS _sqsClient;

    /// <summary>
    /// Amazon S3 client used for uploading messages as objects.
    /// </summary>
    private readonly IAmazonS3 _s3Client;

    /// <summary>
    /// Initializes a new instance of the SqsToS3Service class.
    /// </summary>
    /// <param name="sqsClient">Configured Amazon SQS client.</param>
    /// <param name="s3Client">Configured Amazon S3 client.</param>
    public SqsToS3Service(IAmazonSQS sqsClient, IAmazonS3 s3Client)
    {
        _sqsClient = sqsClient;
        _s3Client = s3Client;
    }

    /// <summary>
    /// Polls a single SQS queue once using long polling and uploads each received message to S3. Optionally, a delegate can be provided to execute custom logic (e.g., logging, metrics) after each message is processed.
    /// </summary>
    /// <param name="queueUrl">The URL of the SQS queue to poll.</param>
    /// <param name="bucketName">The S3 bucket to store messages in.</param>
    /// <param name="maxMessages">Maximum number of messages to receive in one poll (1-10).</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// $1Optional callback invoked after each message is processed. The callback parameters are: queueUrl (string), bucketName (string), message (Amazon.SQS.Model.Message).$3
    /// <returns>The number of messages processed.</returns>
    public async Task<int> PollOnceAsync(
        string queueUrl,
        string bucketName,
        int maxMessages = 10,
        CancellationToken cancellationToken = default,
        Func<string, string, Message, Task>? onMessageProcessed = null)
    {
        var receiveRequest = new ReceiveMessageRequest
        {
            QueueUrl = queueUrl,
            MaxNumberOfMessages = Math.Clamp(maxMessages, 1, 10),
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
                Key = message.MessageId + ".json",
                ContentBody = message.Body
            };

            await _s3Client.PutObjectAsync(putRequest, cancellationToken);
            await _sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle, cancellationToken);

            if (onMessageProcessed != null)
            {
                await onMessageProcessed(queueUrl, bucketName, message);
            }
        }

        return response.Messages.Count;
    }

    /// <summary>
    /// Polls multiple SQS queues once. For each queue, receives messages and writes them to the corresponding S3 bucket. An optional delegate can be passed to execute custom post-processing per message.
    /// </summary>
    /// <param name="queueToBucketMap">A dictionary mapping queue URLs to S3 bucket names.</param>
    /// <param name="maxMessages">Maximum number of messages to receive per queue (1-10).</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// $1Optional callback invoked after each message is processed. Parameters: queueUrl (string) — the SQS queue URL, bucketName (string) — the S3 bucket name, message (Amazon.SQS.Model.Message) — the SQS message object.$3
    /// <returns>Total number of messages processed across all queues.</returns>
    public async Task<int> PollQueuesOnceAsync(
        Dictionary<string, string> queueToBucketMap,
        int maxMessages = 10,
        CancellationToken cancellationToken = default,
        Func<string, string, Message, Task>? onMessageProcessed = null)
    {
        int totalProcessed = 0;

        foreach (var kvp in queueToBucketMap)
        {
            var processed = await PollOnceAsync(kvp.Key, kvp.Value, maxMessages, cancellationToken, onMessageProcessed);
            totalProcessed += processed;
        }

        return totalProcessed;
    }

    /// <summary>
    /// Continuously polls all configured SQS queues at fixed intervals. Each received message is stored in its mapped S3 bucket. Post-processing logic may be applied via an optional callback delegate.
    /// </summary>
    /// <param name="queueToBucketMap">A dictionary mapping queue URLs to S3 bucket names.</param>
    /// <param name="interval">Polling interval between each iteration.</param>
    /// <param name="maxMessages">Maximum number of messages to receive per queue (1-10).</param>
    /// <param name="cancellationToken">Token to stop the polling loop.</param>
    /// $1Optional callback invoked after each message is processed. Parameters: queueUrl (string) — the SQS queue URL, bucketName (string) — the S3 bucket name, message (Amazon.SQS.Model.Message) — the SQS message object.$3
    /// <returns>A task that completes when polling is cancelled.</returns>
    public async Task PollQueuesWithTimerAsync(
        Dictionary<string, string> queueToBucketMap,
        TimeSpan interval,
        int maxMessages = 10,
        CancellationToken cancellationToken = default,
        Func<string, string, Message, Task>? onMessageProcessed = null)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await PollQueuesOnceAsync(queueToBucketMap, maxMessages, cancellationToken, onMessageProcessed);
                await Task.Delay(interval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Polling error: {ex.Message}");
            }
        }
    }
}

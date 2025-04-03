using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System;

public class SqsToS3Service
{
    private readonly IAmazonSQS _sqsClient;
    private readonly IAmazonS3 _s3Client;

    public SqsToS3Service(IAmazonSQS sqsClient, IAmazonS3 s3Client)
    {
        _sqsClient = sqsClient;
        _s3Client = s3Client;
    }

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
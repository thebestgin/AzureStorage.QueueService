using Azure;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using JasonShave.AzureStorage.QueueService.Extensions;
using JasonShave.AzureStorage.QueueService.Interfaces;
using JasonShave.AzureStorage.QueueService.Models;
using Microsoft.Extensions.Logging;

namespace JasonShave.AzureStorage.QueueService.Services;

public sealed class AzureStorageQueueClient
{
    private readonly IMessageConverter _messageConverter;
    private readonly QueueClient _queueClient;
    private readonly ILogger<AzureStorageQueueClient> _logger;

    internal AzureStorageQueueClient(IMessageConverter messageConverter, QueueClient queueClient,
        ILogger<AzureStorageQueueClient> logger)
    {
        _messageConverter = messageConverter;
        _queueClient = queueClient;
        _logger = logger;
    }

    public async ValueTask CreateQueueIfNotExistsAsync(IDictionary<string, string>? metadata = null,
        CancellationToken cancellationToken = default) =>
        await _queueClient.CreateIfNotExistsAsync(metadata, cancellationToken);

    public async ValueTask ClearMessagesAsync(CancellationToken cancellationToken = default) =>
        await _queueClient.ClearMessagesAsync(cancellationToken);

    public async ValueTask<IEnumerable<TMessage>> PeekMessagesAsync<TMessage>(int numMessages,
        CancellationToken cancellationToken = default) =>
        (await _queueClient.PeekMessagesAsync(numMessages, cancellationToken)).Value.Convert<TMessage>(
            _messageConverter);

    public IEnumerable<TMessage>
        PeekMessages<TMessage>(int numMessages, CancellationToken cancellationToken = default) =>
        _queueClient.PeekMessages(numMessages, cancellationToken).Value.Convert<TMessage>(_messageConverter);

    /// <summary>
    /// Receives a message of the type specified and deserializes the input using a JSON message converter.
    /// The message is processed using a function delegate and errors are processed using an exception handling delegate.
    /// </summary>
    /// <typeparam name="TMessage"></typeparam>
    /// <param name="handleMessage"></param>
    /// <param name="handleException"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="numMessages"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public async ValueTask ReceiveMessagesAsync<TMessage>(
        Func<TMessage?, ValueTask> handleMessage,
        Func<Exception, ValueTask> handleException,
        int numMessages = 1,
        CancellationToken cancellationToken = default)
        where TMessage : class
    {
        QueueMessage[] receivedMessages = await _queueClient.ReceiveMessagesAsync(numMessages, null, cancellationToken);

        if (receivedMessages.Any())
        {
            _logger.LogMessageCount(receivedMessages.Length);

            foreach (var queueMessage in receivedMessages)
            {
                await ProcessMessage(queueMessage);
            }

            async Task ProcessMessage(QueueMessage queueMessage)
            {
                try
                {
                    var convertedMessage = _messageConverter.Convert<TMessage>(queueMessage.MessageText);
                    await handleMessage(convertedMessage);

                    _logger.LogProcessedMessage(queueMessage.MessageId);
                    await _queueClient.DeleteMessageAsync(queueMessage.MessageId, queueMessage.PopReceipt,
                        cancellationToken);
                }
                catch (Exception e)
                {
                    await handleException(e);
                }
            }
        }
    }

    public async ValueTask<int> ApproximateMessagesCountAsync(CancellationToken cancellationToken)
    {
        QueueProperties properties = await _queueClient.GetPropertiesAsync(cancellationToken);

        return properties.ApproximateMessagesCount;
    }

    const double AzureMaxNumMessages = 32.0;
    public async ValueTask<int> ReceiveMessagesAsync<TMessage>(
        Func<List<TMessage?>, ValueTask> handlePreprocess,
        Func<TMessage?, ValueTask> handleMessage,
        Func<Exception, TMessage?, string?, ValueTask> handleException,
        Func<List<TMessage?>, List<TMessage?>, ValueTask> handlePostprocess,
        Func<TMessage?, ValueTask<bool>>? handlePostDeletion,
        bool postDeletion = true,
        int numMessages = 1,
        CancellationToken cancellationToken = default(CancellationToken))
        where TMessage : class
    {
        // var processedQueueMessages = new List<QueueMessage>();
        // var deletedQueueMessages = new List<QueueMessage>();
        var exceptionMessages = new List<TMessage?>();
        var processedMessages = new List<TMessage?>();
        QueueMessage[] queueMessages;

        
        if (numMessages > AzureMaxNumMessages)
        {
            var approximateMessages = await ApproximateMessagesCountAsync(cancellationToken);
            if (approximateMessages == 0)
                return 0;

            var requestsCount = Math.Ceiling(approximateMessages / AzureMaxNumMessages);
            var tasks = new List<Task<Response<QueueMessage[]>>>();
            for (var index = 0; index < requestsCount; index++)
            {
                var task = _queueClient.ReceiveMessagesAsync(32, null, cancellationToken);
                tasks.Add(task);
            }

            var batchQueueMessages = new List<QueueMessage>();
            foreach (var task in tasks)
            {
                QueueMessage[] batchItemQueueMessages;
                try
                {
                    batchItemQueueMessages = await task;
                }
                catch (Exception e)
                {
                    _logger.LogError(e, $"{nameof(QueueClient.ReceiveMessagesAsync)} failed.");
                    throw;
                }

                batchQueueMessages.AddRange(batchItemQueueMessages);
            }

            queueMessages = batchQueueMessages.ToArray();
        }
        else
        {
            try
            {
                queueMessages = await _queueClient.ReceiveMessagesAsync(numMessages, null, cancellationToken);
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"{nameof(QueueClient.ReceiveMessagesAsync)} failed.");
                throw;
            }
        }
        
        if (queueMessages.Any())
        {
            _logger.LogMessageCount(queueMessages.Length);

            var messages = new List<TMessage?>();
            foreach (var receivedMessage in queueMessages)
            {
                try
                {
                    var convertedMessage = _messageConverter.Convert<TMessage>(receivedMessage.MessageText);
                    messages.Add(convertedMessage);
                }
                catch (Exception ex)
                {
                    await handleException(ex, null, receivedMessage.MessageId);
                }
            }

            await handlePreprocess(messages);

            for (var index = 0; index < queueMessages.Length; ++index)
                await ProcessMessage(queueMessages[index], messages[index]);

            await handlePostprocess(processedMessages, exceptionMessages);

            // If handlePostprocess throws an exception, PostDeleteMessage is not called
            for (var index = 0; index < queueMessages.Length; ++index)
            {
                await PostDeleteMessage(queueMessages[index], messages[index]!);
            }
        }

        // return new ReceiveMessagesResult()
        // {
        //     RequestedMessageCount = numMessages,
        //     ReceivedMessageCount = queueMessages.Length,
        //     DeletedQueueMessageCount = deletedQueueMessages.Count,
        //     ProcessedMessageCount = processedMessages.Count,
        //     ExceptionMessageCount = exceptionMessages.Count
        // };

        return queueMessages.Length;


        async Task ProcessMessage(QueueMessage queueMessage, TMessage? message)
        {
            try
            {
                await handleMessage(message);

                // processedQueueMessages.Add(queueMessage);
                processedMessages.Add(message);

                _logger.LogProcessedMessage(queueMessage.MessageId);
                if (!postDeletion)
                {
                    if (handlePostDeletion != null)
                    {
                        var delete = await handlePostDeletion(message);
                        if(delete == false)
                            return;
                    }
                    
                    await _queueClient.DeleteMessageAsync(queueMessage.MessageId, queueMessage.PopReceipt,
                        cancellationToken);
                }
            }
            catch (Exception ex)
            {
                // exceptionQueueMessages.Add(queueMessage);
                exceptionMessages.Add(message);
                await handleException(ex, message, queueMessage.MessageId);
            }
        }

        async Task PostDeleteMessage(QueueMessage queueMessage, TMessage message)
        {
            try
            {
                if (handlePostDeletion != null)
                {
                    var delete = await handlePostDeletion(message);
                    if(delete == false)
                        return;
                }
                
                await this._queueClient.DeleteMessageAsync(queueMessage.MessageId, queueMessage.PopReceipt, cancellationToken);
                //deletedQueueMessages.Add(queueMessage);
            }
            catch (Exception ex)
            {
                await handleException(ex, message, queueMessage.MessageId);
            }
        }
    }


    /// <summary>
    /// Sends a message of a specified type and handles serialization of the message to the correct format.
    /// </summary>
    /// <typeparam name="TMessage"></typeparam>
    /// <param name="message"></param>
    /// <param name="cancellationToken"></param>
    /// <returns><see cref="SendResponse"/></returns>
    /// <exception cref="Exception"></exception>
    public async ValueTask<SendResponse> SendMessageAsync<TMessage>(TMessage message,
        CancellationToken cancellationToken = default)
    {
        try
        {
            BinaryData binaryMessage = _messageConverter.Convert(message);
            SendReceipt response = await _queueClient.SendMessageAsync(binaryMessage, null, null, cancellationToken);

            return new SendResponse(response.PopReceipt, response.MessageId);
        }
        catch (Exception e)
        {
            _logger.LogSendError(e.Message);
            throw;
        }
    }
}
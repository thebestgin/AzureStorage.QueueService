namespace JasonShave.AzureStorage.QueueService.Models;

public class ReceiveMessagesResult
{
    public int QueueMessageCount { get; set; }
    public int DeletedQueueMessageCount { get; set; }
    public int ProcessedMessageCount { get; set; }
    public int ExceptionMessageCount { get; set; }
}
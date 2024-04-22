namespace JasonShave.AzureStorage.QueueService.Models;

public class ReceiveMessagesResult
{
    public int RequestedMessageCount { get; set; }
    
    public int ReceivedMessageCount { get; set; }
    public int DeletedQueueMessageCount { get; set; }
    public int ProcessedMessageCount { get; set; }
    public int ExceptionMessageCount { get; set; }

    public bool AreTherePossiblyMoreMessages()
    {
        return ReceivedMessageCount < RequestedMessageCount;
    }
}
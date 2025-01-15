﻿
namespace Order.API.Service
{
    public interface IBus
    {
        Task<bool> Publish<T1, T2>(T1 key, T2 value, string topicOrQueueName);
        Task CreateTopicOrQueue(List<string> topicOrQueueNameList);
    }
}
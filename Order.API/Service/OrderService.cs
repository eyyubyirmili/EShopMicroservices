using Order.API.Dto;
using Shared.Events;
using Shared.Events.Events;

namespace Order.API.Service
{
    public class OrderService(IBus bus)
    {
        public async Task<bool> Create(OrderCreateRequestDto request)
        {
            //Save to database 
            var orderCode = Guid.NewGuid().ToString();


            var orderCreatedEvent = new OrderCreateEvent(orderCode, request.UserId, request.TotalPrice);

           return await bus.Publish(orderCode, orderCreatedEvent, BusConstants.OrderCreatedEventTopicName);

        }
    }
}

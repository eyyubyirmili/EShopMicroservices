namespace Shared.Events.Events
{
    public record OrderCreateEvent(string OrderCode, string UserId, decimal TotalPrice);


}

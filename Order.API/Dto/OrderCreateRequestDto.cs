namespace Order.API.Dto
{
    public record OrderCreateRequestDto(string UserId, decimal TotalPrice);
}

using Order.API.Service;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<IBus, Bus>();
//builder.Services.AddSingleton<IBus, Bus>(sp =>
//{
//    var logger = sp.GetRequiredService<ILogger<Bus>>();
//    var bus = new Bus(builder.Configuration, logger);
//    bus.CreateTopicOrQueue([BusConstants.OrderCreatedEventTopicName]);
//    return bus;
//});
builder.Services.AddScoped<OrderService>();

var app = builder.Build();

await app.CreateTopicsOrQueues();


// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();

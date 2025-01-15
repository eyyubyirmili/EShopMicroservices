﻿using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Order.API.Dto;
using Order.API.Service;

namespace Order.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class OrdersController(OrderService orderService) : ControllerBase
    {
        [HttpPost]
        public async Task<IActionResult> Create(OrderCreateRequestDto request)
        {
            return Ok(await orderService.Create(request));
        }
    }
}

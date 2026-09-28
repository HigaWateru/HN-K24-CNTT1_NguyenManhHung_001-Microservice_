package org.example.orderservice.models.dto.responses;

import org.example.orderservice.models.constants.OrderStatus;
import org.example.orderservice.models.repositories.OrderDetailRepository;

import java.util.List;

public record OrderResponse(
    Long id,
    String customerName,
    String customerEmail,
    Double total,
    OrderStatus status,
    List<OrderDetailRepository> items
) {

}

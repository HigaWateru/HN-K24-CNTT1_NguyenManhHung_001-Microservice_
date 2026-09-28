package org.example.orderservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.orderservice.models.constants.OrderStatus;
import org.example.orderservice.models.dto.requests.CreateOrderDetailRequest;
import org.example.orderservice.models.dto.requests.CreateOrderRequest;
import org.example.orderservice.models.dto.responses.OrderResponse;
import org.example.orderservice.models.dto.responses.ProductResponse;
import org.example.orderservice.models.entities.Order;
import org.example.orderservice.models.entities.OrderDetail;
import org.example.orderservice.models.repositories.OrderDetailRepository;
import org.example.orderservice.models.repositories.OrderRepository;
import org.example.orderservice.models.services.OrderService;
import org.example.orderservice.models.services.impl.ProductGatewayService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

        private final OrderRepository orderRepository;
        private final OrderDetailRepository orderDetailRepository;
        private final ProductGatewayService productGatewayService;
        private final KafkaTemplate<String, Object> kafkaTemplate;

        @Value("${app.kafka.topics.order-created:order-created}")
        private String orderCreatedTopic;

        @Override
        @Transactional
        public OrderResponse createOrder(CreateOrderRequest request) {
//                log.info("Creating order for customer: {}, email: {}", request.customerName(), request.customerEmail());
//
//                double totalAmount = 0.0;
//                List<OrderDetail> detailsToSave = new ArrayList<>();
//
//                for (CreateOrderDetailRequest item : request.items()) {
//                        ProductResponse product = productGatewayService.getProductById(item.productId());
//
//                        if (product.stock() != null && product.stock() < item.quantity()) {
//                                throw new IllegalArgumentException(String.format(
//                                    "product ID %d (%s) only has %d slots available, but %d were requested.",
//                                    product.id(), product.name(), product.stock(), item.quantity()
//                                ));
//                        }
//
//                        Double price = product.price();
//                        Double lineTotal = price * item.quantity();
//                        totalAmount += lineTotal;
//
//                        OrderDetail detail = OrderDetail.builder()
//                            .productId(product.id())
//                            .quantity(item.quantity())
//                            .unitPrice(price)
//                            .build();
//
//                        detailsToSave.add(detail);
//                }
//
//                Order order = Order.builder()
//                    .customerName(request.customerName())
//                    .total(totalAmount)
//                    .status(OrderStatus.SUCCESS)
//                    .build();
////
//                Order savedOrder = orderRepository.save(order);
//
//                for (OrderDetail detail : detailsToSave) {
//                        detail.setOrder(savedOrder);
//                }
//                List<OrderDetail> savedDetails = orderDetailRepository.saveAll(detailsToSave);
//
//                publishOrderCreatedEvent(savedOrder);
//
//                List<OrderDetailRepository> detailResponses = savedDetails.stream()
//                    .map(d -> new OrderDetailRepository(
//                        d.getId(),
//                        d.getProductId(),
//                        d.getQuantity(),
//                        d.getUnitPrice()
//                    ) {
//                    })
//                    .toList();
//
//                return new OrderResponse(
//                    savedOrder.getId(),
//                    savedOrder.getCustomerName(),
//                    savedOrder.getCustomerEmail(),
//                    savedOrder.getTotal(),
//                    savedOrder.getStatus(),
//                    detailResponses
//                );
                return null;
        }

        private void publishOrderCreatedEvent(Order order) {
                try {
                        kafkaTemplate.send(orderCreatedTopic, String.valueOf(order.getId()), order.getCustomerEmail())
                            .whenComplete((result, ex) -> {
                                    if (ex != null) {
                                            log.error("Failed to publish order-created event to Kafka for customer ID: {}",
                                                order.getId(), ex);
                                    } else {
                                            log.info("Successfully published order-created event for order ID: {} to topic: {}",
                                                order.getId(), orderCreatedTopic);
                                    }
                            });
                } catch (Exception e) {
                        log.error("Error occurred while sending Kafka message for appointment ID: {}", order.getId(), e);
                }
        }
}

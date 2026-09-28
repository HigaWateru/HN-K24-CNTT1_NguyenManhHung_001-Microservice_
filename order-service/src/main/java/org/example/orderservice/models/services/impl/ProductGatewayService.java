package org.example.orderservice.models.services.impl;

import feign.FeignException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.orderservice.clients.ProductClient;
import org.example.orderservice.exceptions.ProductNotFoundException;
import org.example.orderservice.exceptions.ProductServiceException;
import org.example.orderservice.models.dto.responses.ProductResponse;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProductGatewayService {
    private final ProductClient productClient;

    @CircuitBreaker(name = "orderService", fallbackMethod = "getProductByIdFallback")
    public ProductResponse getProductById(Long productId) {
        try {
            return productClient.getProductById(productId);
        } catch (FeignException.NotFound e) {
            throw new ProductNotFoundException(productId);
        } catch (FeignException e) {
            log.error("Feign exception while calling product-service for productId {}: {}", productId, e.getMessage());
            throw new ProductServiceException("Product service error: " + e.getMessage(), e);
        }
    }

    public ProductResponse getProductByIdFallback(Long productId, ProductNotFoundException ex) {
        throw ex;
    }

    public ProductResponse getProductByIdFallback(Long productId, Throwable t) {
        log.error("Circuit breaker fallback activated for productId {}. Error: {}", productId, t.getMessage());
        throw new ProductServiceException("Doctor service is currently unavailable. Please try again later.", t);
    }
}

package com.platform.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class ApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApplication.class, args);
    }

    // Rate limiter key: limit per user ID from header, or per IP address
    @Bean
    public KeyResolver userKeyResolver() {
        return exchange -> {
            var userId = exchange.getRequest().getHeaders().getFirst("X-User-Id");
            if (userId != null) return Mono.just(userId);
            var remoteAddress = exchange.getRequest().getRemoteAddress();
            return Mono.just(remoteAddress != null ? remoteAddress.getHostString() : "anonymous");
        };
    }
}

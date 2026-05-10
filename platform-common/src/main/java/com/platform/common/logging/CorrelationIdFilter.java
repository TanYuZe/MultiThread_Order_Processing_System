package com.platform.common.logging;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Injects correlation ID into MDC for every incoming HTTP request.
 *
 * The correlation ID flows through:
 *   1. HTTP header X-Correlation-Id → MDC → all log lines
 *   2. Kafka producer header (see KafkaCorrelationIdProducerInterceptor)
 *   3. Child thread context (see MdcTaskDecorator)
 *
 * Without this filter, every log line has correlationId=null, making
 * distributed tracing across services impossible.
 */
@Component
@Order(1)
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String MDC_CORRELATION_ID = "correlationId";
    public static final String MDC_THREAD_TYPE = "threadType";

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        var correlationId = request.getHeader(CORRELATION_ID_HEADER);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        MDC.put(MDC_CORRELATION_ID, correlationId);
        MDC.put(MDC_THREAD_TYPE, Thread.currentThread().isVirtual() ? "virtual" : "platform");

        response.setHeader(CORRELATION_ID_HEADER, correlationId);

        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.clear();  // CRITICAL: always clear to prevent ThreadLocal leaks in thread pools
        }
    }
}

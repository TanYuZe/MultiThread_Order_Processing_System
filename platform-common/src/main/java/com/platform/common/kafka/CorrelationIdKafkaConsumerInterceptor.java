package com.platform.common.kafka;

import com.platform.common.logging.CorrelationIdFilter;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

/**
 * Reads correlationId from Kafka message headers and restores it in MDC
 * for the consumer thread.
 *
 * This ensures that the consuming service's log lines include the same
 * correlationId as the producing service — enabling end-to-end request tracing
 * across service boundaries.
 *
 * Register in Kafka consumer properties:
 *   spring.kafka.consumer.properties.interceptor.classes=
 *     com.platform.common.kafka.CorrelationIdKafkaConsumerInterceptor
 *
 * NOTE: MDC.clear() must be called after processing each message.
 * This is handled by the @KafkaListener method's try/finally, or by
 * configuring a Kafka listener error handler.
 */
public class CorrelationIdKafkaConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        records.forEach(record -> {
            Header header = record.headers().lastHeader(CorrelationIdKafkaProducerInterceptor.CORRELATION_ID_HEADER);
            String correlationId = header != null
                ? new String(header.value(), StandardCharsets.UTF_8)
                : UUID.randomUUID().toString();  // generate new ID if not propagated
            MDC.put(CorrelationIdFilter.MDC_CORRELATION_ID, correlationId);
            MDC.put(CorrelationIdFilter.MDC_THREAD_TYPE,
                Thread.currentThread().isVirtual() ? "virtual" : "platform");
        });
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {}

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}

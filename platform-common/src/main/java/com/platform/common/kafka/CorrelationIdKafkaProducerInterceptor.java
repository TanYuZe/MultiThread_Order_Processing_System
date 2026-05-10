package com.platform.common.kafka;

import com.platform.common.logging.CorrelationIdFilter;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Injects the correlationId from MDC into Kafka message headers.
 *
 * This bridges the correlation ID across the Kafka boundary — without this,
 * the consuming service has no way to link its processing logs to the originating
 * HTTP request that produced the event.
 *
 * Register in Kafka producer properties:
 *   spring.kafka.producer.properties.interceptor.classes=
 *     com.platform.common.kafka.CorrelationIdKafkaProducerInterceptor
 */
public class CorrelationIdKafkaProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        var correlationId = MDC.get(CorrelationIdFilter.MDC_CORRELATION_ID);
        if (correlationId != null) {
            record.headers().add(new RecordHeader(
                CORRELATION_ID_HEADER,
                correlationId.getBytes(StandardCharsets.UTF_8)
            ));
        }
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {}

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}

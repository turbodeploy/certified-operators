package com.vmturbo.components.api.tracing;

import java.util.Map;

import io.opentracing.contrib.kafka.TracingKafkaUtils;
import io.opentracing.util.GlobalTracer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * A kafka interceptor to inject tracing spans. Will always inject a span when
 * there is an active tracing span unless the DISABLE_KAFKA_TRACES_BAGGAGE_KEY
 * baggage item is set in the current span's tracing context.
 * <p/>
 * {@see https://github.com/opentracing-contrib/java-kafka-client} for more details.
 * {@see TracingProducerInterceptor.class} for an alternative example.
 *
 * @param <K> The key class for the kafka producer.
 * @param <V> The value class for the kafka producer.
 */
public class TracingKafkaProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    /**
     * Create a new {@link TracingKafkaProducerInterceptor}.
     */
    public TracingKafkaProducerInterceptor() {
    }

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
        if (Tracing.isKafkaTracingEnabled()) {
            TracingKafkaUtils.buildAndInjectSpan(producerRecord, GlobalTracer.get()).finish();
        }

        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}

package com.vmturbo.components.api.server;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.AbstractMessage;

import com.vmturbo.communication.CommunicationException;

public class KafkaMessageProducer implements AutoCloseable {

    private final KafkaProducer<String, byte[]> producer;
    private final Logger logger = LogManager.getLogger(getClass());
    private final AtomicLong msgCounter = new AtomicLong(0);

    public KafkaMessageProducer(@Nonnull String bootstrapServer) {
        // set the default properties
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        // pjs: set a max request size based on standard max protobuf serializabe size for now
        props.put("max.request.size",67108864); // 64mb
        props.put("buffer.memory", 67108864);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());

        producer = new KafkaProducer<>(props);
    }

    private Future<RecordMetadata> sendKafkaMessage(@Nonnull final AbstractMessage serverMsg,
            @Nonnull final String topic) {
        Objects.requireNonNull(serverMsg);
        byte[] payload = serverMsg.toByteArray();
        logger.debug("Sending message {}[{} bytes] to topic {}", serverMsg.getClass().getSimpleName(), payload.length, topic);
        return producer.send(
                new ProducerRecord<>(topic, Long.toString(msgCounter.incrementAndGet()), payload));
    }

    @Override
    public void close() {
        producer.close();
    }

    /**
     * Creates message sender for the specific topic.
     *
     * @param topic topic to send messages to
     * @return message sender.
     */
    public <S extends AbstractMessage> IMessageSender<S> messageSender(@Nonnull String topic) {
        return new BusMessageSender<>(topic);
    }

    /**
     * Real implementation of the sender.
     */
    private class BusMessageSender<S extends AbstractMessage> implements IMessageSender<S> {
        private final String topic;

        private BusMessageSender(@Nonnull String topic) {
            this.topic = Objects.requireNonNull(topic);
        }

        @Override
        public void sendMessage(@Nonnull S serverMsg)
                throws CommunicationException, InterruptedException {
            try {
                sendKafkaMessage(serverMsg, topic).get();
            } catch (ExecutionException e) {
                throw new CommunicationException("Unexpected exception sending message " +
                        serverMsg.getClass().getSimpleName(), e);
            }
        }
    }
}

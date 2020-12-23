package com.vmturbo.mediation.actionstream.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import javax.annotation.Nonnull;

import com.google.protobuf.AbstractMessage;
import com.googlecode.protobuf.format.JsonFormat;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Producer class sends messages to external kafka.
 */
public class ActionStreamKafkaProducer {

    /**
     * {@link KafkaProducer} publishes records to the Kafka cluster.
     */
    private final KafkaProducer<String, String> producer;
    private final Logger logger = LogManager.getLogger(getClass());
    private final JsonFormat jsonFormat = new JsonFormat();

    /**
     * Constructor of {@link ActionStreamKafkaProducer}.
     *
     * @param properties configuration properties for {@link KafkaProducer}
     */
    public ActionStreamKafkaProducer(@Nonnull Properties properties) {
        producer = new KafkaProducer<>(properties);
    }

    /**
     * Send message to certain kafka topic.
     *
     * @param serverMsg message to send
     * @param topic topic name
     */
    public void sendMessage(@Nonnull final AbstractMessage serverMsg, @Nonnull final String topic) {
        final Instant startTime = Instant.now();
        logger.debug("Sending message {} to topic {}.", serverMsg.getClass().getSimpleName(),
                topic);
        final String message = jsonFormat.printToString(serverMsg);
        producer.send(new ProducerRecord<>(topic, message), (metadata, exception) -> {
            double sentTimeMs = Duration.between(startTime, Instant.now()).toMillis();
            logger.debug("Message send took {} ms", sentTimeMs);
            if (exception != null) {
                logger.warn("Error while sending kafka message", exception);
            }
        });
    }

    /**
     * Close {@link KafkaProducer}.
     */
    public void close() {
        producer.close();
    }
}

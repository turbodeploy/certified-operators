package com.vmturbo.extractor.export;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.extractor.schema.json.export.ExportedObject;

/**
 * Class for sending {@link ExportedObject}s to a Kafka topic. We sent objects to Kafka
 * asynchronously for better performance.
 *
 * <p>The format of the object we send to Kafka is limited by the available plugins. Currently
 * customers are using a logstash plugin which reads kafka topic and write to elasticsearch, it
 * supports only two json types: one is JSON array (list of objects), another is newline-delimited
 * JSON. maybe we can try the newline-delimited option. But some customers are using Splunk, they
 * use "Splunk Connect for Kafka", not sure whether it supports newline-delimited JSON. Since the
 * performance is fine now, we should check if it can be optimized later.
 */
public class ExtractorKafkaSender {

    private static final Logger logger = LogManager.getLogger();

    private final IMessageSender<byte[]> kafkaMessageSender;
    private final int kafkaTimeoutSeconds;

    /**
     * Constructor for {@link ExtractorKafkaSender}.
     *
     * @param kafkaMessageSender for sending objects to kafka
     * @param kafkaTimeoutSeconds max time to wait for an object to be delivered to kafka
     */
    public ExtractorKafkaSender(IMessageSender<byte[]> kafkaMessageSender, int kafkaTimeoutSeconds) {
        this.kafkaMessageSender = kafkaMessageSender;
        this.kafkaTimeoutSeconds = kafkaTimeoutSeconds;
    }

    /**
     * Send the given collection of {@link ExportedObject} to Kafka.
     *
     * @param exportedObjects collection of {@link ExportedObject}
     * @return number of objects successfully sent to Kafka
     */
    public int send(@Nonnull Collection<ExportedObject> exportedObjects) {
        final List<Future<?>> futures = new ArrayList<>();
        final List<String> objectsWithSerializedErrors = new ArrayList<>();
        exportedObjects.forEach(exportedObject -> {
            try {
                byte[] bytes = ExportUtils.toBytes(exportedObject);
                futures.add(kafkaMessageSender.sendMessageAsync(bytes));
            } catch (JsonProcessingException e) {
                // track objects which can not be serialized
                objectsWithSerializedErrors.add(exportedObject.toString());
            }
        });

        if (!objectsWithSerializedErrors.isEmpty()) {
            logger.error("{} of {} objects can not be serialized: {}",
                    objectsWithSerializedErrors.size(), exportedObjects.size(), objectsWithSerializedErrors);
        }

        final MutableInt successCounter = new MutableInt(0);
        final MutableInt executionExceptionCounter = new MutableInt(0);
        final MutableInt timeoutExceptionCounter = new MutableInt(0);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        try {
            for (Future<?> future : futures) {
                try {
                    future.get(kafkaTimeoutSeconds, TimeUnit.SECONDS);
                    successCounter.increment();
                } catch (ExecutionException e) {
                    if (executionExceptionCounter.intValue() == 0) {
                        e.printStackTrace(pw);
                    }
                    executionExceptionCounter.increment();
                } catch (TimeoutException e) {
                    if (timeoutExceptionCounter.intValue() == 0) {
                        e.printStackTrace(pw);
                    }
                    timeoutExceptionCounter.increment();
                }
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while sending objects to Kafka: {} of {} sent",
                    successCounter.intValue(), futures.size(), e);
        } finally {
            // Log summary of exceptions with count to avoid log pollution.
            if (executionExceptionCounter.intValue() != 0 || timeoutExceptionCounter.intValue() != 0) {
                logger.error("Failed to send {} objects to kafka due to ExecutionException and {} objects due to TimeoutException, stacktrace of first exception of each follows: {}",
                    executionExceptionCounter.intValue(), timeoutExceptionCounter.intValue(),  sw.toString());
            }

        }
        return successCounter.intValue();
    }

}

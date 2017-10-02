package com.vmturbo.components.test.utilities.metric.writer;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricMetadata;
import com.vmturbo.components.test.utilities.metric.MetricsWarehouseVisitor;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricFamilyKey;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricKey;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.TimestampedSample;

/**
 * A visitor that creates a JSON document out of the visited metrics. For debugging/logging.
 */
public class JsonMetricsWarehouseVisitor implements MetricsWarehouseVisitor {

    private final JsonFactory jsonFactory = new JsonFactory();


    private final Writer writer;
    private final JsonGenerator generator;

    public JsonMetricsWarehouseVisitor() throws IOException {
        writer = new StringWriter();
        generator = jsonFactory.createGenerator(writer);

    }

    @Override
    public void onStartWarehouse(@Nonnull final String testName) {
        try {
            generator.writeStartObject();
            generator.writeStringField("testName", testName);
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected IOException with StringWriter.", e);
        }
    }

    @Override
    public void onEndWarehouse() {
        try {
            generator.writeEndObject();
            generator.close();
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected IOException with StringWriter.", e);
        }
    }

    @Override
    public void onStartHarvester(@Nonnull final String harvesterName) {
        try {
            generator.writeArrayFieldStart(harvesterName);
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected IOException with StringWriter.", e);
        }
    }

    @Override
    public void onEndHarvester() {
        try {
            generator.writeEndArray();
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected IOException with StringWriter.", e);
        }
    }

    @Override
    public void onStartMetricFamily(@Nonnull final MetricFamilyKey key) {
        try {
            generator.writeStartObject();
            generator.writeStringField("name", key.getName());
            generator.writeStringField("type", key.getType().toString());
            generator.writeStringField("help", key.getHelp());
            generator.writeArrayFieldStart("metric");
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected IOException with StringWriter.", e);
        }
    }

    @Override
    public void onEndMetricFamily() {
        try {
            generator.writeEndArray();
            generator.writeEndObject();
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected IOException with StringWriter.", e);
        }
    }

    @Override
    public void onStartMetric(@Nonnull final MetricKey key,
                              @Nonnull final MetricMetadata metadata) {
        try {
            generator.writeStartObject();
            generator.writeObjectFieldStart("labels");
            key.getLabels().forEach((labelKey, labelVal) -> {
                try {
                    generator.writeStringField(labelKey, labelVal);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            });
            generator.writeEndObject(); // END labels
            generator.writeArrayFieldStart("samples");
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected IOException with StringWriter.", e);
        }
    }

    @Override
    public void onEndMetric() {
        try {
            generator.writeEndArray();
            generator.writeEndObject();
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected IOException with StringWriter.", e);
        }

    }

    @Override
    public void onSample(@Nonnull final TimestampedSample sample) {
        try {
            generator.writeStartObject();
            generator.writeNumberField("time", sample.timeMs);
            generator.writeNumberField("value", sample.value);
            generator.writeEndObject();
        } catch (IOException e) {
            throw new IllegalStateException("Unexpected IOException with StringWriter.", e);
        }
    }

    public String getJsonString() {
        return writer.toString();
    }
}

package com.vmturbo.common.protobuf.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilterOrBuilder;
import com.vmturbo.common.protobuf.utils.ProtobufSummarizer.FieldOverride;

/**
 * Tests of {@link ProtobufSummarizer} class.
 */
public class ProtobufSummarizerTest {

    /**
     * Make sure that list elision works correctly, including edge cases.
     */
    @Test
    public void testArrayLimiting() {
        List<Long> values = Arrays.asList(1L, 2L, 3L, 4L);
        assertThat(ProtobufSummarizer.limitArray(values, 0), is("[...+4]"));
        assertThat(ProtobufSummarizer.limitArray(values, 1), is("[1,...+3]"));
        assertThat(ProtobufSummarizer.limitArray(values, 2), is("[1,2,...+2]"));
        assertThat(ProtobufSummarizer.limitArray(values, 3), is("[1,2,3,...+1]"));
        assertThat(ProtobufSummarizer.limitArray(values, 4), is("[1,2,3,4]"));
        assertThat(ProtobufSummarizer.limitArray(values, 5), is("[1,2,3,4]"));
        final List<Long> empty = Collections.emptyList();
        assertThat(ProtobufSummarizer.limitArray(empty, 0), is("[]"));
        assertThat(ProtobufSummarizer.limitArray(empty, 1), is("[]"));
    }

    /**
     * Test that field exclusions work correctly.
     */
    @Test
    public void testFieldExclusions() {
        final StatsFilter sf = StatsFilter.newBuilder()
                .setStartDate(1L)
                .addCommodityRequests(CommodityRequest.newBuilder()
                        .setCommodityName(StringConstants.CPU)
                        .build())
                .build();
        ProtobufSummarizer.Builder<StatsFilterOrBuilder> builder =
                ProtobufSummarizer.of(StatsFilterOrBuilder.class, StatsFilter.class);
        assertThat(builder.build().summarize(sf),
                is("{\"commodity_requests\":[{\"commodityName\":\"CPU\"}],\"start_date\":1}"));
        assertThat(builder.excludingFields("start_date").build().summarize(sf),
                is("{\"commodity_requests\":[{\"commodityName\":\"CPU\"}]}"));
        assertThat(builder.excludingFields("commodity_requests", "start_date").build().summarize(sf),
                is("{}"));
    }

    /**
     * Test that field overrides work correctly.
     */
    @Test
    public void testThatFieldValueOverridesWork() {
        final StatsFilter sf = StatsFilter.newBuilder()
                .setStartDate(1L)
                .addCommodityRequests(CommodityRequest.newBuilder()
                        .setCommodityName(StringConstants.CPU)
                        .build())
                .build();
        // we display a formatted start timestamp instead of the raw epoch-millis value
        FieldOverride formatDate = (name, value) -> Instant.ofEpochMilli((long)value).toString();

        ProtobufSummarizer<StatsFilterOrBuilder> summarizer =
                ProtobufSummarizer.of(StatsFilterOrBuilder.class, StatsFilter.class)
                        .overriding("start_date", formatDate)
                        .build();

        assertThat(summarizer.summarize(sf),
                is("{\"commodity_requests\":[{\"commodityName\":\"CPU\"}],"
                        + "\"start_date\":1970-01-01T00:00:00.001Z}"));
    }

    /**
     * Test that fields set to their default values are excluded.
     */
    @Test
    public void testThatDefaultValuesAreExcluded() {
        ProtobufSummarizer<StatsFilterOrBuilder> summarizer =
                ProtobufSummarizer.of(StatsFilterOrBuilder.class, StatsFilter.class)
                .excludingDefaultValues()
                .build();
        StatsFilter.Builder builder = StatsFilter.newBuilder()
                .setRequestProjectedHeadroom(false);
        assertThat(summarizer.summarize(builder), is("{}"));
        builder.setRequestProjectedHeadroom(true);
        assertThat(summarizer.summarize(builder), is("{\"requestProjectedHeadroom\":true}"));
    }
}

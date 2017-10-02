package com.vmturbo.components.test.utilities.metric.writer;

import java.time.Clock;
import java.util.Collections;

import org.junit.Test;
import org.mockito.Mockito;
import org.skyscreamer.jsonassert.JSONAssert;

import com.vmturbo.components.test.utilities.metric.MetricTestUtil;
import com.vmturbo.components.test.utilities.metric.MetricTestUtil.TestMetricsScraper;
import com.vmturbo.components.test.utilities.metric.MetricsWarehouse;

public class HarvestJsonWriterTest {

    @Test
    public void testJsonWriter() throws Exception {
        Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenReturn(1000L);

        TestMetricsScraper testHarvester = new TestMetricsScraper(Collections.singletonList(
                MetricTestUtil.createSimpleFamilyWithSamples("family",
                        MetricTestUtil.createOneLabelSample(10),
                        MetricTestUtil.createNoLabelSample(10))), clock);
        MetricsWarehouse farm = MetricsWarehouse.newBuilder()
            .addScraper(testHarvester)
            .build();
        testHarvester.scrape(false);

        JsonMetricsWarehouseVisitor writer = new JsonMetricsWarehouseVisitor();
        farm.visit("testJsonWriter", writer);
        String jsonStr = writer.getJsonString();

        JSONAssert.assertEquals(
                "{\"testName\":\"testJsonWriter\",\"test\":[{" +
                    "\"name\":\"family\"," +
                    "\"type\":\"COUNTER\"," +
                    "\"help\":\"help\"," +
                    "\"metric\":[" +
                        "{\"labels\":{\"name\":\"value\"}," +
                        "\"samples\":[{\"time\":1000,\"value\":10.0}]}," +
                        "{\"labels\":{}," +
                        "\"samples\":[{\"time\":1000,\"value\":10.0}]}" +
                        "]}]}",
                jsonStr, false);
    }
}

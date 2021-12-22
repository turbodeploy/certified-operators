package com.vmturbo.reserved.instance.coverage.allocator.metrics;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.junit.Test;

import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.reserved.instance.coverage.allocator.topology.ServiceProviderInfo;

public class RICoverageAllocationMetricsProviderTest {

    private static final ServiceProviderInfo AWS_SERVICE_PROVIDER_INFO = ServiceProviderInfo.builder()
            .oid(1L)
            .name("AWS")
            .build();

    @Test
    public void testTotalCoverageAnalysisDuration() {

        final DataMetricSummary metricSummary =
                DataMetricSummary.builder()
                        .withName("testTotalCoverageAnalysisDuration")
                        .withHelp("testTotalCoverageAnalysisDuration help")
                        .build();

        final RICoverageAllocationMetricsProvider metricsProvider =
                RICoverageAllocationMetricsProvider.newBuilder()
                        .totalCoverageAnalysisDuration(metricSummary)
                        .build();

        assertThat(metricsProvider.totalCoverageAnalysisDuration(), is(not(nullValue())));
        assertThat(metricsProvider.totalCoverageAnalysisDuration().get(), is(not(nullValue())));
    }

    @Test
    public void testFirstPassRIFilterDuration() {

        final DataMetricSummary metricSummary =
                DataMetricSummary.builder()
                        .withName("firstPassRIFilterDuration")
                        .withHelp("firstPassRIFilterDuration help")
                        .build();

        final RICoverageAllocationMetricsProvider metricsProvider =
                RICoverageAllocationMetricsProvider.newBuilder()
                        .firstPassRIFilterDuration(metricSummary)
                        .build();

        assertThat(metricsProvider.firstPassRIFilterDuration(), is(not(nullValue())));
        assertThat(metricsProvider.firstPassRIFilterDuration().get(), is(not(nullValue())));
    }

    @Test
    public void testFirstPassEntityFilterDuration() {

        final DataMetricSummary metricSummary =
                DataMetricSummary.builder()
                        .withName("firstPassEntityFilterDuration")
                        .withHelp("firstPassEntityFilterDuration help")
                        .build();

        final RICoverageAllocationMetricsProvider metricsProvider =
                RICoverageAllocationMetricsProvider.newBuilder()
                        .firstPassEntityFilterDuration(metricSummary)
                        .build();

        assertThat(metricsProvider.firstPassEntityFilterDuration(), is(not(nullValue())));
        assertThat(metricsProvider.firstPassEntityFilterDuration().get(), is(not(nullValue())));
    }

    @Test
    public void testContextCreationDuration() {

        final DataMetricSummary metricSummary =
                DataMetricSummary.builder()
                        .withName("contextCreationDuration")
                        .withHelp("contextCreationDuration help")
                        .build();

        final RICoverageAllocationMetricsProvider metricsProvider =
                RICoverageAllocationMetricsProvider.newBuilder()
                        .contextCreationDuration(metricSummary)
                        .build();

        assertThat(metricsProvider.contextCreationDuration(), is(not(nullValue())));
        assertThat(metricsProvider.contextCreationDuration().get(), is(not(nullValue())));
    }

    @Test
    public void testCoverableEntityCountForCSP() {

        final DataMetricSummary metricSummary =
                RICoverageAllocationMetricsProvider.newCloudServiceProviderMetric()
                        .withName("coverableEntityCountForCSP")
                        .withHelp("coverableEntityCountForCSP help")
                        .build();

        final RICoverageAllocationMetricsProvider metricsProvider =
                RICoverageAllocationMetricsProvider.newBuilder()
                        .coverableEntityCountByCSP(metricSummary)
                        .build();

        assertThat(metricsProvider.coverableEntityCountForCSP(AWS_SERVICE_PROVIDER_INFO), is(not(nullValue())));
        assertThat(metricsProvider.coverableEntityCountForCSP(AWS_SERVICE_PROVIDER_INFO).get(), is(not(nullValue())));
    }

    @Test
    public void testReservedInstanceCountForCSP() {

        final DataMetricSummary metricSummary =
                RICoverageAllocationMetricsProvider.newCloudServiceProviderMetric()
                        .withName("reservedInstanceCountForCSP")
                        .withHelp("reservedInstanceCountForCSP help")
                        .build();

        final RICoverageAllocationMetricsProvider metricsProvider =
                RICoverageAllocationMetricsProvider.newBuilder()
                        .reservedInstanceCountByCSP(metricSummary)
                        .build();

        assertThat(metricsProvider.reservedInstanceCountForCSP(AWS_SERVICE_PROVIDER_INFO), is(not(nullValue())));
        assertThat(metricsProvider.reservedInstanceCountForCSP(AWS_SERVICE_PROVIDER_INFO).get(), is(not(nullValue())));
    }

    @Test
    public void testUncoveredEntityCapacityForCSP() {

        final DataMetricSummary metricSummary =
                RICoverageAllocationMetricsProvider.newCloudServiceProviderMetric()
                        .withName("uncoveredEntityCapacityForCSP")
                        .withHelp("uncoveredEntityCapacityForCSP help")
                        .build();

        final RICoverageAllocationMetricsProvider metricsProvider =
                RICoverageAllocationMetricsProvider.newBuilder()
                        .uncoveredEntityCapacityByCSP(metricSummary)
                        .build();

        assertThat(metricsProvider.uncoveredEntityCapacityForCSP(AWS_SERVICE_PROVIDER_INFO), is(not(nullValue())));
        assertThat(metricsProvider.uncoveredEntityCapacityForCSP(AWS_SERVICE_PROVIDER_INFO).get(), is(not(nullValue())));
    }

    @Test
    public void testUnallocatedRICapacityForCSP() {

        final DataMetricSummary metricSummary =
                RICoverageAllocationMetricsProvider.newCloudServiceProviderMetric()
                        .withName("unallocatedRICapacityForCSP")
                        .withHelp("unallocatedRICapacityForCSP help")
                        .build();

        final RICoverageAllocationMetricsProvider metricsProvider =
                RICoverageAllocationMetricsProvider.newBuilder()
                        .unallocatedRICapacityByCSP(metricSummary)
                        .build();

        assertThat(metricsProvider.unallocatedRICapacityForCSP(AWS_SERVICE_PROVIDER_INFO), is(not(nullValue())));
        assertThat(metricsProvider.unallocatedRICapacityForCSP(AWS_SERVICE_PROVIDER_INFO).get(), is(not(nullValue())));
    }

    @Test
    public void testAllocationDurationForCSP() {

        final DataMetricSummary metricSummary =
                RICoverageAllocationMetricsProvider.newCloudServiceProviderMetric()
                        .withName("allocationDurationForCSP")
                        .withHelp("allocationDurationForCSP help")
                        .build();

        final RICoverageAllocationMetricsProvider metricsProvider =
                RICoverageAllocationMetricsProvider.newBuilder()
                        .allocationDurationByCSP(metricSummary)
                        .build();

        assertThat(metricsProvider.allocationDurationForCSP(AWS_SERVICE_PROVIDER_INFO), is(not(nullValue())));
        assertThat(metricsProvider.allocationDurationForCSP(AWS_SERVICE_PROVIDER_INFO).get(), is(not(nullValue())));
    }

    @Test
    public void testAllocationCountForCSP() {

        final DataMetricSummary metricSummary =
                RICoverageAllocationMetricsProvider.newCloudServiceProviderMetric()
                        .withName("allocationCountForCSP")
                        .withHelp("allocationCountForCSP help")
                        .build();

        final RICoverageAllocationMetricsProvider metricsProvider =
                RICoverageAllocationMetricsProvider.newBuilder()
                        .allocationCountByCSP(metricSummary)
                        .build();

        assertThat(metricsProvider.allocationCountForCSP(AWS_SERVICE_PROVIDER_INFO), is(not(nullValue())));
        assertThat(metricsProvider.allocationCountForCSP(AWS_SERVICE_PROVIDER_INFO).get(), is(not(nullValue())));
    }

    @Test
    public void testAllocatedCoverageCountForCSP() {

        final DataMetricSummary metricSummary =
                RICoverageAllocationMetricsProvider.newCloudServiceProviderMetric()
                        .withName("allocatedCoverageCountForCSP")
                        .withHelp("allocatedCoverageCountForCSP help")
                        .build();

        final RICoverageAllocationMetricsProvider metricsProvider =
                RICoverageAllocationMetricsProvider.newBuilder()
                        .allocatedCoverageAmountByCSP(metricSummary)
                        .build();

        assertThat(metricsProvider.allocatedCoverageAmountForCSP(AWS_SERVICE_PROVIDER_INFO), is(not(nullValue())));
        assertThat(metricsProvider.allocatedCoverageAmountForCSP(AWS_SERVICE_PROVIDER_INFO).get(), is(not(nullValue())));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCSPMetric() {
        final DataMetricSummary metricSummary =
                DataMetricSummary.builder()
                        .withName("testInvalidCSPMetric")
                        .withHelp("testInvalidCSPMetric help")
                        .build();

        // This should throw an exception due to not having a CSP label for the metric
        RICoverageAllocationMetricsProvider.newBuilder()
                .allocatedCoverageAmountByCSP(metricSummary)
                .build();
    }
}

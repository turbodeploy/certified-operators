package com.vmturbo.reserved.instance.coverage.allocator.metrics;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.junit.Test;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.reserved.instance.coverage.allocator.topology.ServiceProviderInfo;

public class CoverageAllocationMetricsProviderTest {

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

        final CoverageAllocationMetricsProvider metricsProvider =
                CoverageAllocationMetricsProvider.newBuilder()
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

        final CoverageAllocationMetricsProvider metricsProvider =
                CoverageAllocationMetricsProvider.newBuilder()
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

        final CoverageAllocationMetricsProvider metricsProvider =
                CoverageAllocationMetricsProvider.newBuilder()
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

        final CoverageAllocationMetricsProvider metricsProvider =
                CoverageAllocationMetricsProvider.newBuilder()
                        .contextCreationDuration(metricSummary)
                        .build();

        assertThat(metricsProvider.contextCreationDuration(), is(not(nullValue())));
        assertThat(metricsProvider.contextCreationDuration().get(), is(not(nullValue())));
    }

    @Test
    public void testCoverableEntityCountForCSP() {
        final DataMetricSummary metricSummary =
                CoverageAllocationMetricsProvider.newCloudServiceProviderMetric()
                        .withName("coverableEntityCountForCSP")
                        .withHelp("coverableEntityCountForCSP help")
                        .build();

        final CoverageAllocationMetricsProvider metricsProvider =
                CoverageAllocationMetricsProvider.newBuilder()
                        .coverableEntityCountByCSP(metricSummary)
                        .build();

        assertThat(metricsProvider.coverableEntityCountForCSP(AWS_SERVICE_PROVIDER_INFO), is(not(nullValue())));
        assertThat(metricsProvider.coverableEntityCountForCSP(AWS_SERVICE_PROVIDER_INFO).get(), is(not(nullValue())));
    }

    @Test
    public void testCloudCommitmentCount() {

        final DataMetricSummary metricSummary =
                CoverageAllocationMetricsProvider.newCloudServiceProviderMetric()
                        .withName("reservedInstanceCountForCSP")
                        .withHelp("reservedInstanceCountForCSP help")
                        .build();

        final CoverageAllocationMetricsProvider metricsProvider =
                CoverageAllocationMetricsProvider.newBuilder()
                        .cloudCommitmentCount(metricSummary)
                        .build();

        assertThat(metricsProvider.cloudCommitmentCount(AWS_SERVICE_PROVIDER_INFO), is(not(nullValue())));
        assertThat(metricsProvider.cloudCommitmentCount(AWS_SERVICE_PROVIDER_INFO).get(), is(not(nullValue())));
    }

    @Test
    public void testAllocationDurationForCSP() {

        final DataMetricSummary metricSummary =
                CoverageAllocationMetricsProvider.newCloudServiceProviderMetric()
                        .withName("allocationDurationForCSP")
                        .withHelp("allocationDurationForCSP help")
                        .build();

        final CoverageAllocationMetricsProvider metricsProvider =
                CoverageAllocationMetricsProvider.newBuilder()
                        .allocationDurationByCSP(metricSummary)
                        .build();

        assertThat(metricsProvider.allocationDurationForCSP(AWS_SERVICE_PROVIDER_INFO), is(not(nullValue())));
        assertThat(metricsProvider.allocationDurationForCSP(AWS_SERVICE_PROVIDER_INFO).get(), is(not(nullValue())));
    }

    @Test
    public void testAllocationCountForCSP() {

        final DataMetricSummary metricSummary =
                CoverageAllocationMetricsProvider.newCloudServiceProviderMetric()
                        .withName("allocationCountForCSP")
                        .withHelp("allocationCountForCSP help")
                        .build();

        final CoverageAllocationMetricsProvider metricsProvider =
                CoverageAllocationMetricsProvider.newBuilder()
                        .allocationCountByCSP(metricSummary)
                        .build();

        assertThat(metricsProvider.allocationCountForCSP(AWS_SERVICE_PROVIDER_INFO), is(not(nullValue())));
        assertThat(metricsProvider.allocationCountForCSP(AWS_SERVICE_PROVIDER_INFO).get(), is(not(nullValue())));
    }

    @Test
    public void testAllocatedCoverageCountForCSP() {

        final CloudCommitmentCoverageTypeInfo coverageTypeInfo = CloudCommitmentCoverageTypeInfo.newBuilder()
                .setCoverageType(CloudCommitmentCoverageType.COUPONS)
                .build();

        final DataMetricSummary metricSummary =
                CoverageAllocationMetricsProvider.newCoverageTypeMetric()
                        .withName("allocatedCoverageCountForCSP")
                        .withHelp("allocatedCoverageCountForCSP help")
                        .build();

        final CoverageAllocationMetricsProvider metricsProvider =
                CoverageAllocationMetricsProvider.newBuilder()
                        .allocatedCoverageAmount(metricSummary)
                        .build();

        assertThat(metricsProvider.allocatedCoverageAmount(AWS_SERVICE_PROVIDER_INFO, coverageTypeInfo), is(not(nullValue())));
        assertThat(metricsProvider.allocatedCoverageAmount(AWS_SERVICE_PROVIDER_INFO, coverageTypeInfo).get(), is(not(nullValue())));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCSPMetric() {
        final DataMetricSummary metricSummary =
                DataMetricSummary.builder()
                        .withName("testInvalidCSPMetric")
                        .withHelp("testInvalidCSPMetric help")
                        .build();

        // This should throw an exception due to not having a CSP label for the metric
        CoverageAllocationMetricsProvider.newBuilder()
                .allocatedCoverageAmount(metricSummary)
                .build();
    }
}

package com.vmturbo.reserved.instance.coverage.allocator.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.reserved.instance.coverage.allocator.topology.ServiceProviderInfo;

public class CoverageAllocationMetricsCollectorTest {

    private static final ServiceProviderInfo AWS_SERVICE_PROVIDER_INFO = ServiceProviderInfo.builder()
            .oid(1L)
            .name("AWS")
            .build();

    private final DataMetricTimerProvider dataMetricTimerProvider = mock(DataMetricTimerProvider.class);
    private final CoverageAllocationMetricsProvider metricsProvider =
            mock(CoverageAllocationMetricsProvider.class);
    private final CoverageAllocationMetricsCollector metricsCollector =
            new CoverageAllocationMetricsCollector(metricsProvider);

    @Test
    public void testOnCoverageAnalysis() {

        when(metricsProvider.totalCoverageAnalysisDuration())
                .thenReturn(Optional.of(dataMetricTimerProvider));

        final CoverageAllocationOperation result = metricsCollector.onCoverageAnalysis();

        assertThat(result, instanceOf(InstrumentedAllocationOperation.class));
    }

    @Test
    public void testOnCoverageAnalysisEmptyMetric() {

        when(metricsProvider.totalCoverageAnalysisDuration())
                .thenReturn(Optional.empty());

        final CoverageAllocationOperation result = metricsCollector.onCoverageAnalysis();

        assertThat(result, instanceOf(InstrumentedAllocationOperation.class));
    }

    @Test
    public void testOnFirstPassRIFilter() {

        when(metricsProvider.firstPassRIFilterDuration())
                .thenReturn(Optional.of(dataMetricTimerProvider));

        final CoverageAllocationOperation result = metricsCollector.onFirstPassCoverageFilter();

        assertThat(result, instanceOf(InstrumentedAllocationOperation.class));
    }

    @Test
    public void testOnFirstPassRIFilterEmptyMetric() {

        when(metricsProvider.firstPassRIFilterDuration())
                .thenReturn(Optional.empty());

        final CoverageAllocationOperation result = metricsCollector.onFirstPassCoverageFilter();

        assertThat(result, equalTo(CoverageAllocationOperation.PASS_THROUGH_OPERATION));
    }

    @Test
    public void testOnFirstPassEntityFilter() {

        when(metricsProvider.firstPassEntityFilterDuration())
                .thenReturn(Optional.of(dataMetricTimerProvider));

        final CoverageAllocationOperation result = metricsCollector.onFirstPassEntityFilter();

        assertThat(result, instanceOf(InstrumentedAllocationOperation.class));
    }

    @Test
    public void testOnFirstPassEntityFilterEmptyMetric() {

        when(metricsProvider.firstPassEntityFilterDuration())
                .thenReturn(Optional.empty());

        final CoverageAllocationOperation result = metricsCollector.onFirstPassEntityFilter();

        assertThat(result, equalTo(CoverageAllocationOperation.PASS_THROUGH_OPERATION));
    }

    @Test
    public void testOnContextCreation() {

        when(metricsProvider.contextCreationDuration())
                .thenReturn(Optional.of(dataMetricTimerProvider));

        final CoverageAllocationOperation result = metricsCollector.onContextCreation();

        assertThat(result, instanceOf(InstrumentedAllocationOperation.class));
    }

    @Test
    public void testOnContextCreationEmptyMetric() {

        when(metricsProvider.contextCreationDuration())
                .thenReturn(Optional.empty());

        final CoverageAllocationOperation result = metricsCollector.onContextCreation();

        assertThat(result, equalTo(CoverageAllocationOperation.PASS_THROUGH_OPERATION));
    }

    //TODO:ejf
//    @Test
//    public void testOnCoverageAnalysisForCSP() {
//
//        /*
//        Setup mocks
//         */
//        final Set<Long> coverableEntityOids = ImmutableSet.of(1L, 2L);
//        final Set<Long> reservedInstanceOids = ImmutableSet.of(4L, 5L, 6L);
//        final Map<Long, Long> entityUncoveredCapacity = ImmutableMap.of(
//                1L, 10L,
//                2L, 20L);
//        final Map<Long, Long> riUnallocatedCapacity = ImmutableMap.of(
//                4L,10L,
//                5L, 20L,
//                6L, 50L);
//
//        final CloudProviderCoverageContext coverageContext = mock(CloudProviderCoverageContext.class);
//        when(coverageContext.serviceProviderInfo()).thenReturn(AWS_SERVICE_PROVIDER_INFO);
//        when(coverageContext.coverableEntityOids()).thenReturn(coverableEntityOids);
//        when(coverageContext.cloudCommitmentOids()).thenReturn(reservedInstanceOids);
//
//        final CloudCommitmentCoverageJournal coverageJournal = mock(CloudCommitmentCoverageJournal.class);
//        when(coverageJournal.getUncoveredCapacity(anyLong())).thenAnswer((mockInvocation) ->
//                entityUncoveredCapacity.getOrDefault(mockInvocation.getArgumentAt(0, Long.class), 0L));
//        when(coverageJournal.getUnallocatedCapacity(anyLong())).thenAnswer((mockInvocation) ->
//                riUnallocatedCapacity.getOrDefault(mockInvocation.getArgumentAt(0, Long.class), 0L));
//
//        final SummaryData coverableEntityCountMetric = mock(SummaryData.class);
//        final SummaryData reservedInstanceCountMetric = mock(SummaryData.class);
//        final SummaryData uncoveredEntityCapacityMetric = mock(SummaryData.class);
//        final SummaryData unallocatedRICapacityMetric = mock(SummaryData.class);
//
//        when(metricsProvider.coverableEntityCountForCSP(eq(csp)))
//                .thenReturn(Optional.of(coverableEntityCountMetric));
//        when(metricsProvider.reservedInstanceCountForCSP(eq(csp)))
//                .thenReturn(Optional.of(reservedInstanceCountMetric));
//        when(metricsProvider.uncoveredEntityCapacityForCSP(eq(csp)))
//                .thenReturn(Optional.of(uncoveredEntityCapacityMetric));
//        when(metricsProvider.unallocatedRICapacityForCSP(eq(csp)))
//                .thenReturn(Optional.of(unallocatedRICapacityMetric));
//        when(metricsProvider.allocationDurationForCSP(eq(csp)))
//                .thenReturn(Optional.of(dataMetricTimerProvider));
//
//        /*
//        Invoke SUT
//         */
//        metricsCollector.onCoverageAnalysisForCSP(coverageContext, coverageJournal);
//
//        /*
//        Setup arg captors
//         */
//        final ArgumentCaptor<Double> coverableEntityCountCaptor = ArgumentCaptor.forClass(Double.class);
//        verify(coverableEntityCountMetric).observe(coverableEntityCountCaptor.capture());
//        final ArgumentCaptor<Double> reservedInstanceCountCaptor = ArgumentCaptor.forClass(Double.class);
//        verify(reservedInstanceCountMetric).observe(reservedInstanceCountCaptor.capture());
//        final ArgumentCaptor<Double> uncoveredEntityCapacityCaptor = ArgumentCaptor.forClass(Double.class);
//        verify(uncoveredEntityCapacityMetric).observe(uncoveredEntityCapacityCaptor.capture());
//        final ArgumentCaptor<Double> unallocatedRICapacityCaptor = ArgumentCaptor.forClass(Double.class);
//        verify(unallocatedRICapacityMetric).observe(unallocatedRICapacityCaptor.capture());
//
//        /*
//        Assertions
//         */
//        assertThat(coverableEntityCountCaptor.getValue(), equalTo((double)coverableEntityOids.size()));
//        assertThat(reservedInstanceCountCaptor.getValue(), equalTo((double)reservedInstanceOids.size()));
//        assertThat(uncoveredEntityCapacityCaptor.getValue(), equalTo(
//                entityUncoveredCapacity.values()
//                        .stream()
//                        .mapToDouble(Double::valueOf)
//                        .sum()));
//        assertThat(unallocatedRICapacityCaptor.getValue(), equalTo(
//                riUnallocatedCapacity.values()
//                        .stream()
//                        .mapToDouble(Double::valueOf)
//                        .sum()));
//    }
//
//    @Test
//    public void testOnCoverageAssignment() {
//
//
//        /*
//        Setup mocks
//         */
//        final CloudCommitmentCoverageJournal coverageJournal = mock(CloudCommitmentCoverageJournal.class);
//        final CloudProviderCoverageContext awsCoverageContext = mock(CloudProviderCoverageContext.class);
//        final CloudProviderCoverageContext azureCoverageContext = mock(CloudProviderCoverageContext.class);
//        when(awsCoverageContext.cloudServiceProvider()).thenReturn(CloudServiceProvider.AWS);
//        when(azureCoverageContext.cloudServiceProvider()).thenReturn(CloudServiceProvider.AZURE);
//
//        // ignore collecting metrics (return empty)
//        when(metricsProvider.totalCoverageAnalysisDuration()).thenReturn(Optional.empty());
//        when(metricsProvider.coverableEntityCountForCSP(any()))
//                .thenReturn(Optional.empty());
//        when(metricsProvider.reservedInstanceCountForCSP(any()))
//                .thenReturn(Optional.empty());
//        when(metricsProvider.uncoveredEntityCapacityForCSP(any()))
//                .thenReturn(Optional.empty());
//        when(metricsProvider.unallocatedRICapacityForCSP(any()))
//                .thenReturn(Optional.empty());
//
//        // returns null on startTimer() - that's okay
//        when(metricsProvider.allocationDurationForCSP(any()))
//                .thenReturn(Optional.of(dataMetricTimerProvider));
//
//        // setup mocks for onAnalysisCompletion()
//        final SummaryData allocationCountAwsMetric = mock(SummaryData.class);
//        final SummaryData allocationCountAzureMetric = mock(SummaryData.class);
//        final SummaryData allocatedCoverageCountAwsMetric = mock(SummaryData.class);
//        final SummaryData allocatedCoverageCountAzureMetric = mock(SummaryData.class);
//
//        when(metricsProvider.allocationCountForCSP(eq(CloudServiceProvider.AWS)))
//                .thenReturn(Optional.of(allocationCountAwsMetric));
//        when(metricsProvider.allocationCountForCSP(eq(CloudServiceProvider.AZURE)))
//                .thenReturn(Optional.of(allocationCountAzureMetric));
//        when(metricsProvider.allocatedCoverageAmountForCSP(eq(CloudServiceProvider.AWS)))
//                .thenReturn(Optional.of(allocatedCoverageCountAwsMetric));
//        when(metricsProvider.allocatedCoverageAmountForCSP(eq(CloudServiceProvider.AZURE)))
//                .thenReturn(Optional.of(allocatedCoverageCountAzureMetric));
//
//
//        /*
//        Setup coverage journals
//         */
//        final CoverageJournalEntry awsCoverageJournalA = CoverageJournalEntry.of(
//                CloudServiceProvider.AWS,
//                "",
//                1L,
//                2L,
//                0L,
//                0L,
//                4L);
//        final CoverageJournalEntry awsCoverageJournalB = CoverageJournalEntry.of(
//                CloudServiceProvider.AWS,
//                "",
//                1L,
//                3L,
//                0L,
//                0L,
//                8L);
//        final CoverageJournalEntry awsCoverageJournalC = CoverageJournalEntry.of(
//                CloudServiceProvider.AWS,
//                "",
//                2L,
//                3L,
//                0L,
//                0L,
//                16L);
//        final CoverageJournalEntry azureCoverageJournalA = CoverageJournalEntry.of(
//                CloudServiceProvider.AZURE,
//                "",
//                4L,
//                5L,
//                0L,
//                0L,
//                20L);
//
//
//        /**
//         * Invoke SUT
//         */
//        metricsCollector.onCoverageAnalysis().observe(() -> {
//            metricsCollector.onCoverageAnalysisForCSP(awsCoverageContext, coverageJournal)
//                    .observe(() -> {
//                        metricsCollector.onCoverageAssignment(awsCoverageJournalA);
//                        metricsCollector.onCoverageAssignment(awsCoverageJournalB);
//                        metricsCollector.onCoverageAssignment(awsCoverageJournalC);
//                    });
//            metricsCollector.onCoverageAnalysisForCSP(azureCoverageContext, coverageJournal)
//                    .observe(() ->
//                        metricsCollector.onCoverageAssignment(azureCoverageJournalA));
//        });
//
//        /*
//        Setup arg captors
//         */
//        final ArgumentCaptor<Double> allocationCountAwsCaptor = ArgumentCaptor.forClass(Double.class);
//        verify(allocationCountAwsMetric).observe(allocationCountAwsCaptor.capture());
//        final ArgumentCaptor<Double> allocationCountAzureCaptor = ArgumentCaptor.forClass(Double.class);
//        verify(allocationCountAzureMetric).observe(allocationCountAzureCaptor.capture());
//        final ArgumentCaptor<Double> allocatedCoverageCountAwsCaptor = ArgumentCaptor.forClass(Double.class);
//        verify(allocatedCoverageCountAwsMetric).observe(allocatedCoverageCountAwsCaptor.capture());
//        final ArgumentCaptor<Double> allocatedCoverageCountAzureCaptor = ArgumentCaptor.forClass(Double.class);
//        verify(allocatedCoverageCountAzureMetric).observe(allocatedCoverageCountAzureCaptor.capture());
//
//        /*
//        Assertions
//         */
//        assertThat(allocationCountAwsCaptor.getValue(), equalTo(3.0));
//        assertThat(allocatedCoverageCountAwsCaptor.getValue(), equalTo(28.0));
//        assertThat(allocationCountAzureCaptor.getValue(), equalTo(1.0));
//        assertThat(allocatedCoverageCountAzureCaptor.getValue(), equalTo(20.0));
//    }
}

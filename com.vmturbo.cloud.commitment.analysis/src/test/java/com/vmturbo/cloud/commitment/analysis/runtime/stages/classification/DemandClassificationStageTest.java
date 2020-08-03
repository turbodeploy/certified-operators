package com.vmturbo.cloud.commitment.analysis.runtime.stages.classification;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.demand.EntityComputeTierAllocation;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableEntityComputeTierAllocation;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableTimeInterval;
import com.vmturbo.cloud.commitment.analysis.demand.TimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage.StageResult;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.AllocatedDemandClassifier.AllocatedDemandClassifierFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate.DemandTimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.CloudTierFamilyMatcher.CloudTierFamilyMatcherFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassificationStage.DemandClassificationFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.selection.EntityCloudTierDemandSet;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.selection.ImmutableEntityCloudTierDemandSet;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecMatcher;
import com.vmturbo.cloud.commitment.analysis.topology.MinimalCloudTopology;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.ClassifiedDemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandClassification.AllocatedClassificationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandClassification.ClassifiedDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class DemandClassificationStageTest {

    private final AllocatedDemandClassifierFactory allocatedDemandClassifierFactory =
            mock(AllocatedDemandClassifierFactory.class);

    private final CloudTierFamilyMatcherFactory cloudTierFamilyMatcherFactory =
            mock(CloudTierFamilyMatcherFactory.class);

    private final DemandClassificationFactory demandClassificationFactory =
            new DemandClassificationFactory(allocatedDemandClassifierFactory, cloudTierFamilyMatcherFactory);

    private final CloudCommitmentSpecMatcher cloudCommitmentSpecMatcher = mock(CloudCommitmentSpecMatcher.class);
    private final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);


    private final CloudTierFamilyMatcher cloudTierFamilyMatcher = mock(CloudTierFamilyMatcher.class);

    private final AllocatedDemandClassifier allocatedDemandClassifier = mock(AllocatedDemandClassifier.class);

    @Before
    public void setup() {
        when(allocatedDemandClassifierFactory.newClassifier(any(), anyLong())).thenReturn(allocatedDemandClassifier);
        when(cloudTierFamilyMatcherFactory.newFamilyMatcher(any())).thenReturn(cloudTierFamilyMatcher);

        when(analysisContext.getCloudCommitmentSpecMatcher()).thenReturn(cloudCommitmentSpecMatcher);
    }


    @Test
    public void testConstruction() {

        final long id = 123L;
        final long minEntityUptime = 100L;
        final CloudCommitmentAnalysisConfig analysisConfig = TestUtils.createBaseConfig()
                .toBuilder()
                .setDemandClassification(DemandClassification.newBuilder()
                        .setDemandSelection(ClassifiedDemandSelection.newBuilder()
                                .addScope(ClassifiedDemandScope.newBuilder()
                                        .setScope(DemandScope.newBuilder())
                                        .addAllocatedDemandClassification(AllocatedDemandClassification.ALLOCATED)))
                        .setAllocatedClassificationSettings(AllocatedClassificationSettings.newBuilder()
                                .setMinEntityUptime(minEntityUptime)))
                .build();


        // construct the stage
        final AnalysisStage<EntityCloudTierDemandSet, ClassifiedEntityDemandSet> demandClassificationStage =
                demandClassificationFactory.createStage(
                        id,
                        analysisConfig,
                        analysisContext);

        // verify the cloud tier family matcher construction
        final ArgumentCaptor<CloudCommitmentSpecMatcher> specMatcherCaptor =
                ArgumentCaptor.forClass(CloudCommitmentSpecMatcher.class);
        verify(cloudTierFamilyMatcherFactory).newFamilyMatcher(specMatcherCaptor.capture());
        assertThat(specMatcherCaptor.getValue(), equalTo(cloudCommitmentSpecMatcher));

        // verify allocated demand classifier construction
        final ArgumentCaptor<CloudTierFamilyMatcher> familyMatcherCaptor =
                ArgumentCaptor.forClass(CloudTierFamilyMatcher.class);
        final ArgumentCaptor<Long> minUptimeCaptor = ArgumentCaptor.forClass(Long.class);
        verify(allocatedDemandClassifierFactory).newClassifier(
                familyMatcherCaptor.capture(),
                minUptimeCaptor.capture());
        assertThat(familyMatcherCaptor.getValue(), equalTo(cloudTierFamilyMatcher));
        assertThat(minUptimeCaptor.getValue(), equalTo(minEntityUptime));
    }

    @Test
    public void testClassification() {

        // Create 4 entity allocation records (2 each for 2 entities)
        final EntityComputeTierAllocation entityAllocationA1 = ImmutableEntityComputeTierAllocation.builder()
                .entityOid(1L)
                .accountOid(2L)
                .regionOid(3L)
                .serviceProviderOid(4L)
                .timeInterval(ImmutableTimeInterval.builder()
                        .startTime(Instant.now().minusSeconds(1000))
                        .endTime(Instant.now().minusSeconds(900))
                        .build())
                .cloudTierDemand(ImmutableComputeTierDemand.builder()
                        .cloudTierOid(5L)
                        .osType(OSType.RHEL)
                        .tenancy(Tenancy.DEFAULT)
                        .build())
                .build();
        final EntityComputeTierAllocation entityAllocationA2 =
                ImmutableEntityComputeTierAllocation.copyOf(entityAllocationA1)
                        .withTimeInterval(ImmutableTimeInterval.builder()
                                .startTime(Instant.now().minusSeconds(700))
                                .endTime(Instant.now())
                                .build());

        final EntityComputeTierAllocation entityAllocationB1 =
                ImmutableEntityComputeTierAllocation.copyOf(entityAllocationA1)
                        .withEntityOid(6L);
        final EntityComputeTierAllocation entityAllocationB2 =
                ImmutableEntityComputeTierAllocation.copyOf(entityAllocationB1)
                        .withTimeInterval(ImmutableTimeInterval.builder()
                                .startTime(Instant.now().minusSeconds(700))
                                .endTime(Instant.now())
                                .build());


        // setup the cloud topology
        final MinimalCloudTopology cloudTopology = mock(MinimalCloudTopology.class);
        when(cloudTopology.getEntity(anyLong())).thenReturn(Optional.empty());
        when(analysisContext.getSourceCloudTopology()).thenReturn(cloudTopology);

        // setup allocated demand classifier
        final Map<AllocatedDemandClassification, Set<DemandTimeSeries>> classifiedEntityA =
                ImmutableMap.of(
                        AllocatedDemandClassification.EPHEMERAL, Collections.singleton(
                                ImmutableDemandTimeSeries.builder()
                                        .cloudTierDemand(entityAllocationA1.cloudTierDemand())
                                        .demandIntervals(TimeSeries.newTimeline(
                                                Collections.singleton(entityAllocationA1.timeInterval())))
                                        .build()),
                        AllocatedDemandClassification.ALLOCATED, Collections.singleton(
                                ImmutableDemandTimeSeries.builder()
                                        .cloudTierDemand(entityAllocationA2.cloudTierDemand())
                                        .demandIntervals(TimeSeries.newTimeline(
                                                Collections.singleton(entityAllocationA2.timeInterval())))
                                        .build()));

        final Map<AllocatedDemandClassification, Set<DemandTimeSeries>> classifiedEntityB =
                ImmutableMap.of(
                        AllocatedDemandClassification.ALLOCATED, Collections.singleton(
                                ImmutableDemandTimeSeries.builder()
                                        .cloudTierDemand(entityAllocationB1.cloudTierDemand())
                                        .demandIntervals(TimeSeries.newTimeline(
                                                Lists.newArrayList(
                                                        entityAllocationB1.timeInterval(),
                                                        entityAllocationB2.timeInterval())))
                                        .build()));

        when(allocatedDemandClassifier.classifyEntityDemand(any()))
                .thenAnswer((invocation) -> {
                    final EntityCloudTierMapping entityCloudTierMapping =
                            (EntityCloudTierMapping)invocation.getArgumentAt(0, TimeSeries.class).first();
                    if (entityCloudTierMapping.entityOid() == entityAllocationA1.entityOid()) {
                        return classifiedEntityA;
                    } else {
                        return classifiedEntityB;
                    }
                });

        // setup EntityCloudTierDemandSet input
        final EntityCloudTierDemandSet entityCloudTierDemandSet = ImmutableEntityCloudTierDemandSet.builder()
                .addAllocatedDemand(entityAllocationA1, entityAllocationA2, entityAllocationB1, entityAllocationB2)
                .build();


        // construct and invoke the stage
        final AnalysisStage<EntityCloudTierDemandSet, ClassifiedEntityDemandSet> demandClassificationStage =
                demandClassificationFactory.createStage(
                        123L,
                        TestUtils.createBaseConfig(),
                        analysisContext);
        final StageResult<ClassifiedEntityDemandSet> stageResult =
                demandClassificationStage.execute(entityCloudTierDemandSet);


        // verify the invocations of the allocated demand classifier
        final ArgumentCaptor<TimeSeries> entityTimeSeries =
                ArgumentCaptor.forClass(TimeSeries.class);
        verify(allocatedDemandClassifier, times(2)).classifyEntityDemand(entityTimeSeries.capture());
        assertThat(entityTimeSeries.getAllValues(), containsInAnyOrder(
                TimeSeries.newTimeSeries(Lists.newArrayList(entityAllocationA1, entityAllocationA2)),
                TimeSeries.newTimeSeries(Lists.newArrayList(entityAllocationB1, entityAllocationB2))));

        // verify the result
        final ClassifiedEntityDemandAggregate entityAggregateA = ImmutableClassifiedEntityDemandAggregate.builder()
                .entityOid(entityAllocationA1.entityOid())
                .accountOid(entityAllocationA1.accountOid())
                .regionOid(entityAllocationA1.regionOid())
                .serviceProviderOid(entityAllocationA1.serviceProviderOid())
                .putAllClassifiedCloudTierDemand(classifiedEntityA)
                .build();
        final ClassifiedEntityDemandAggregate entityAggregateB = ImmutableClassifiedEntityDemandAggregate.builder()
                .entityOid(entityAllocationB1.entityOid())
                .accountOid(entityAllocationB1.accountOid())
                .regionOid(entityAllocationB1.regionOid())
                .serviceProviderOid(entityAllocationB1.serviceProviderOid())
                .putAllClassifiedCloudTierDemand(classifiedEntityB)
                .build();
        final ClassifiedEntityDemandSet expectedDemandSet = ImmutableClassifiedEntityDemandSet.builder()
                .addClassifiedAllocatedDemand(entityAggregateA, entityAggregateB)
                .build();

        assertThat(stageResult.output(), equalTo(expectedDemandSet));
        assertThat(stageResult.resultSummary(), not(isEmptyOrNullString()));
    }
}

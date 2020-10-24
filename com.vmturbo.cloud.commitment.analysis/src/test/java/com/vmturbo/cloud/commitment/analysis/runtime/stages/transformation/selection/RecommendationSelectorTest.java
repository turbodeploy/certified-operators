package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate.DemandTimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ClassificationFilter.ClassificationFilterFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.CloudTierFilter.CloudTierFilterFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.EntityStateFilter.EntityStateFilterFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.RecommendationSelector.RecommendationSelectorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ScopedEntityFilter.ScopedEntityFilterFactory;
import com.vmturbo.cloud.common.data.TimeSeries;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandSelection;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class RecommendationSelectorTest {

    private final CloudTierFilter cloudTierFilter = mock(CloudTierFilter.class);
    private final CloudTierFilterFactory cloudTierFilterFactory = mock(CloudTierFilterFactory.class);
    private final ScopedEntityFilter scopedEntityFilter = mock(ScopedEntityFilter.class);
    private final ScopedEntityFilterFactory scopedEntityFilterFactory = mock(ScopedEntityFilterFactory.class);
    private final EntityStateFilter entityStateFilter = mock(EntityStateFilter.class);
    private final EntityStateFilterFactory entityStateFilterFactory = mock(EntityStateFilterFactory.class);
    private final ClassificationFilter classificationFilter = mock(ClassificationFilter.class);
    private final ClassificationFilterFactory classificationFilterFactory = mock(ClassificationFilterFactory.class);

    private final RecommendationSelectorFactory recommendationSelectorFactory = new RecommendationSelectorFactory(
            cloudTierFilterFactory,
            scopedEntityFilterFactory,
            entityStateFilterFactory,
            classificationFilterFactory,
            true);


    private final DemandClassification allocatedClassification = DemandClassification.of(
            AllocatedDemandClassification.ALLOCATED);
    private final DemandTimeSeries allocatedSeries = DemandTimeSeries.builder()
            .cloudTierDemand(ComputeTierDemand.builder()
                    .cloudTierOid(5)
                    .osType(OSType.RHEL)
                    .tenancy(Tenancy.DEFAULT)
                    .build())
            .demandIntervals(TimeSeries.newTimeline())
            .build();
    private final DemandClassification flexibleClassification = DemandClassification.of(
            AllocatedDemandClassification.FLEXIBLY_ALLOCATED);
    private final DemandTimeSeries flexibleSeries = DemandTimeSeries.builder()
            .cloudTierDemand(ComputeTierDemand.builder()
                    .cloudTierOid(6)
                    .osType(OSType.RHEL)
                    .tenancy(Tenancy.DEFAULT)
                    .build())
            .demandIntervals(TimeSeries.newTimeline())
            .build();
    private final ClassifiedEntityDemandAggregate entityAggregate = ClassifiedEntityDemandAggregate.builder()
            .entityOid(1)
            .accountOid(2)
            .regionOid(3)
            .serviceProviderOid(4)
            .putClassifiedCloudTierDemand(
                    allocatedClassification,
                    Collections.singleton(allocatedSeries))
            .putClassifiedCloudTierDemand(
                    flexibleClassification,
                    Collections.singleton(flexibleSeries))
            .build();

    private final ClassifiedEntitySelection allocatedSelection = ClassifiedEntitySelection.builder()
            .entityOid(entityAggregate.entityOid())
            .accountOid(entityAggregate.accountOid())
            .regionOid(entityAggregate.regionOid())
            .serviceProviderOid(entityAggregate.serviceProviderOid())
            .isSuspended(entityAggregate.isSuspended())
            .isTerminated(entityAggregate.isTerminated())
            .cloudTierDemand(allocatedSeries.cloudTierDemand())
            .demandTimeline(allocatedSeries.demandIntervals())
            .classification(allocatedClassification)
            .isRecommendationCandidate(false)
            .build();

    private final ClassifiedEntitySelection flexibleSelection = ClassifiedEntitySelection.builder()
            .from(allocatedSelection)
            .cloudTierDemand(flexibleSeries.cloudTierDemand())
            .demandTimeline(flexibleSeries.demandIntervals())
            .classification(flexibleClassification)
            .build();

    @Before
    public void setup() {
        when(cloudTierFilterFactory.newFilter(any())).thenReturn(cloudTierFilter);
        when(scopedEntityFilterFactory.createFilter(any())).thenReturn(scopedEntityFilter);
        when(entityStateFilterFactory.newFilter(anyBoolean(), anyBoolean())).thenReturn(entityStateFilter);
        when(classificationFilterFactory.newFilter(any())).thenReturn(classificationFilter);
    }

    @Test
    public void testScopeEntityRejection() {
        when(scopedEntityFilter.filter(any())).thenReturn(false);

        final AllocatedDemandSelection demandSelection = AllocatedDemandSelection.newBuilder().build();
        final RecommendationSelector recommendationSelector = recommendationSelectorFactory
                .fromDemandSelection(demandSelection);

        final Set<ClassifiedEntitySelection> actualEntitySelection =
                recommendationSelector.selectRecommendationDemand(entityAggregate);

        assertThat(actualEntitySelection, hasSize(2));
        assertThat(actualEntitySelection, containsInAnyOrder(allocatedSelection, flexibleSelection));

    }

    @Test
    public void testEntityStateRejection() {
        when(scopedEntityFilter.filter(any())).thenReturn(true);
        when(entityStateFilter.filterDemand(any())).thenReturn(false);

        final AllocatedDemandSelection demandSelection = AllocatedDemandSelection.newBuilder().build();
        final RecommendationSelector recommendationSelector = recommendationSelectorFactory
                .fromDemandSelection(demandSelection);

        final Set<ClassifiedEntitySelection> actualEntitySelection =
                recommendationSelector.selectRecommendationDemand(entityAggregate);

        assertThat(actualEntitySelection, hasSize(2));
        assertThat(actualEntitySelection, containsInAnyOrder(allocatedSelection, flexibleSelection));

    }

    @Test
    public void testClassificationFilter() {

        when(scopedEntityFilter.filter(any())).thenReturn(true);
        when(entityStateFilter.filterDemand(any())).thenReturn(true);
        when(cloudTierFilter.filter(any())).thenReturn(true);
        when(classificationFilter.filter(eq(allocatedClassification))).thenReturn(true);
        when(classificationFilter.filter(eq(flexibleClassification))).thenReturn(false);

        final AllocatedDemandSelection demandSelection = AllocatedDemandSelection.newBuilder().build();
        final RecommendationSelector recommendationSelector = recommendationSelectorFactory
                .fromDemandSelection(demandSelection);

        final Set<ClassifiedEntitySelection> actualEntitySelection =
                recommendationSelector.selectRecommendationDemand(entityAggregate);

        final ClassifiedEntitySelection recommendedAllocatedSelection = ClassifiedEntitySelection.builder()
                .from(allocatedSelection)
                .isRecommendationCandidate(true)
                .build();
        assertThat(actualEntitySelection, hasSize(2));
        assertThat(actualEntitySelection, containsInAnyOrder(recommendedAllocatedSelection, flexibleSelection));
    }

    @Test
    public void testCloudTierFilter() {

        when(scopedEntityFilter.filter(any())).thenReturn(true);
        when(entityStateFilter.filterDemand(any())).thenReturn(true);
        when(classificationFilter.filter(any())).thenReturn(true);
        when(cloudTierFilter.filter(eq(allocatedSeries.cloudTierDemand()))).thenReturn(true);
        when(cloudTierFilter.filter(eq(flexibleSeries.cloudTierDemand()))).thenReturn(false);

        final AllocatedDemandSelection demandSelection = AllocatedDemandSelection.newBuilder().build();
        final RecommendationSelector recommendationSelector = recommendationSelectorFactory
                .fromDemandSelection(demandSelection);

        final Set<ClassifiedEntitySelection> actualEntitySelection =
                recommendationSelector.selectRecommendationDemand(entityAggregate);

        final ClassifiedEntitySelection recommendedAllocatedSelection = ClassifiedEntitySelection.builder()
                .from(allocatedSelection)
                .isRecommendationCandidate(true)
                .build();
        assertThat(actualEntitySelection, hasSize(2));
        assertThat(actualEntitySelection, containsInAnyOrder(recommendedAllocatedSelection, flexibleSelection));
    }
}

package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate.DemandTimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationJournal;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.AnalysisSelector.AnalysisSelectorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ClassificationFilter.ClassificationFilterFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.EntityStateFilter.EntityStateFilterFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandSelection;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class AnalysisSelectorTest {

    final ClassificationFilter classificationFilter = mock(ClassificationFilter.class);
    final ClassificationFilterFactory classificationFilterFactory = mock(ClassificationFilterFactory.class);

    final EntityStateFilter entityStateFilter = mock(EntityStateFilter.class);
    final EntityStateFilterFactory entityStateFilterFactory = mock(EntityStateFilterFactory.class);

    final AnalysisSelectorFactory analysisSelectorFactory = new AnalysisSelectorFactory(
            classificationFilterFactory,
            entityStateFilterFactory);

    private final Set<DemandTimeSeries> flexibleSeriesSet = Collections.singleton(DemandTimeSeries.builder()
            .cloudTierDemand(ComputeTierDemand.builder()
                    .cloudTierOid(5)
                    .osType(OSType.RHEL)
                    .tenancy(Tenancy.DEFAULT)
                    .build())
            .build());
    private final ClassifiedEntityDemandAggregate entityAggregate = ClassifiedEntityDemandAggregate.builder()
            .entityOid(1)
            .accountOid(2)
            .regionOid(3)
            .serviceProviderOid(4)
            .putClassifiedCloudTierDemand(
                    DemandClassification.of(AllocatedDemandClassification.ALLOCATED),
                    Collections.singleton(DemandTimeSeries.builder()
                            .cloudTierDemand(ComputeTierDemand.builder()
                                    .cloudTierOid(5)
                                    .osType(OSType.RHEL)
                                    .tenancy(Tenancy.DEFAULT)
                                    .build())
                            .build()))
            .putClassifiedCloudTierDemand(
                    DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED),
                    flexibleSeriesSet)
            .build();


    final DemandTransformationJournal transformationJournal = mock(DemandTransformationJournal.class);

    @Before
    public void setup() {
        when(classificationFilterFactory.newFilter(any())).thenReturn(classificationFilter);
        when(entityStateFilterFactory.newFilter(anyBoolean(), anyBoolean()))
                .thenReturn(entityStateFilter);
    }

    @Test
    public void testEntityStateFilterRejection() {

        when(entityStateFilter.filterDemand(any())).thenReturn(false);

        final AllocatedDemandSelection demandSelection = AllocatedDemandSelection.newBuilder().build();

        final AnalysisSelector analysisSelector = analysisSelectorFactory.newSelector(
                transformationJournal, demandSelection);
        final ClassifiedEntityDemandAggregate selectedAggregate =
                analysisSelector.filterEntityAggregate(entityAggregate);

        // verify the transformation journal was invoked
        final ArgumentCaptor<ClassifiedEntityDemandAggregate> aggregateCaptor =
                ArgumentCaptor.forClass(ClassifiedEntityDemandAggregate.class);
        verify(transformationJournal).recordSkippedEntity(aggregateCaptor.capture());
        assertThat(aggregateCaptor.getValue(), equalTo(entityAggregate));

        // verify all demand has been dropped
        final ClassifiedEntityDemandAggregate expectedAggregate = ClassifiedEntityDemandAggregate.builder()
                .from(entityAggregate)
                .classifiedCloudTierDemand(Collections.emptyMap())
                .build();
        assertThat(selectedAggregate, equalTo(expectedAggregate));
    }


    @Test
    public void testNoIgnoredDemand() {

        when(entityStateFilter.filterDemand(any())).thenReturn(true);
        when(classificationFilter.filter(any())).thenReturn(true);

        final AllocatedDemandSelection demandSelection = AllocatedDemandSelection.newBuilder().build();

        final AnalysisSelector analysisSelector = analysisSelectorFactory.newSelector(
                transformationJournal, demandSelection);
        final ClassifiedEntityDemandAggregate selectedAggregate =
                analysisSelector.filterEntityAggregate(entityAggregate);

        // no changes should be made to the entity aggregate
        assertThat(selectedAggregate, equalTo(entityAggregate));
    }

    @Test
    public void testIgnoredFlexibleDemand() {
        when(entityStateFilter.filterDemand(any())).thenReturn(true);
        when(classificationFilter.filter(eq(DemandClassification.of(AllocatedDemandClassification.ALLOCATED))))
                .thenReturn(true);

        when(classificationFilter.filter(eq(DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED))))
                .thenReturn(false);

        final AllocatedDemandSelection demandSelection = AllocatedDemandSelection.newBuilder().build();

        final AnalysisSelector analysisSelector = analysisSelectorFactory.newSelector(
                transformationJournal, demandSelection);
        final ClassifiedEntityDemandAggregate selectedAggregate =
                analysisSelector.filterEntityAggregate(entityAggregate);

        // check the transformation journal invocation
        final ArgumentCaptor<ClassifiedEntityDemandAggregate> aggregateCaptor =
                ArgumentCaptor.forClass(ClassifiedEntityDemandAggregate.class);
        final ArgumentCaptor<DemandClassification> classificationCaptor =
                ArgumentCaptor.forClass(DemandClassification.class);
        final ArgumentCaptor<Set> demandSetCaptor = ArgumentCaptor.forClass(Set.class);
        verify(transformationJournal).recordSkippedDemand(
                aggregateCaptor.capture(),
                classificationCaptor.capture(),
                demandSetCaptor.capture());
        assertThat(aggregateCaptor.getValue(), equalTo(entityAggregate));
        assertThat(classificationCaptor.getValue(),
                equalTo(DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED)));
        assertThat(demandSetCaptor.getValue(), equalTo(flexibleSeriesSet));

        // verify the flexible demand has been dropped
        assertTrue(selectedAggregate.classifiedCloudTierDemand().containsKey(
                DemandClassification.of(AllocatedDemandClassification.ALLOCATED)));
        assertFalse(selectedAggregate.classifiedCloudTierDemand().containsKey(
                DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED)));
    }
}

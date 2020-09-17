package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ClassificationFilter.ClassificationFilterFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandSelection;

public class ClassificationFilterTest {

    private final ClassificationFilterFactory classificationFilterFactory = new ClassificationFilterFactory();

    @Test
    public void testFlexibleDemandSelection() {

        final AllocatedDemandSelection allocatedDemandSelection = AllocatedDemandSelection.newBuilder()
                .setIncludeFlexibleDemand(true)
                .build();

        final ClassificationFilter classificationFilter = classificationFilterFactory.newFilter(
                allocatedDemandSelection);

        assertTrue(classificationFilter.filter(DemandClassification.of(AllocatedDemandClassification.ALLOCATED)));
        assertTrue(classificationFilter.filter(DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED)));
        assertFalse(classificationFilter.filter(DemandClassification.of(AllocatedDemandClassification.EPHEMERAL)));
    }

    @Test
    public void testAllocatedDemandOnly() {

        final AllocatedDemandSelection allocatedDemandSelection = AllocatedDemandSelection.newBuilder()
                .setIncludeFlexibleDemand(false)
                .build();

        final ClassificationFilter classificationFilter = classificationFilterFactory.newFilter(
                allocatedDemandSelection);

        assertTrue(classificationFilter.filter(DemandClassification.of(AllocatedDemandClassification.ALLOCATED)));
        assertFalse(classificationFilter.filter(DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED)));
    }

}

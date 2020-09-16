package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.EntityStateFilter.EntityStateFilterFactory;

public class EntityStateFilterTest {


    private final ClassifiedEntityDemandAggregate baseAggregate = ClassifiedEntityDemandAggregate.builder()
            .entityOid(1)
            .accountOid(2)
            .regionOid(3)
            .serviceProviderOid(4)
            .isSuspended(false)
            .isTerminated(false)
            .build();

    private final EntityStateFilterFactory entityStateFilterFactory = new EntityStateFilterFactory();


    @Test
    public void testExcludeSuspended() {

        final ClassifiedEntityDemandAggregate entityAggregate = ClassifiedEntityDemandAggregate.builder()
                .from(baseAggregate)
                .isSuspended(true)
                .build();

        final EntityStateFilter entityStateFilter = entityStateFilterFactory.newFilter(
                false, true);

        assertFalse(entityStateFilter.filterDemand(entityAggregate));
    }

    @Test
    public void testExcludeTerminated() {

        final ClassifiedEntityDemandAggregate entityAggregate = ClassifiedEntityDemandAggregate.builder()
                .from(baseAggregate)
                .isTerminated(true)
                .build();

        final EntityStateFilter entityStateFilter = entityStateFilterFactory.newFilter(
                true, false);

        assertFalse(entityStateFilter.filterDemand(entityAggregate));
    }

    @Test
    public void testExcludeSuspendedAndTerminated() {

        final EntityStateFilter entityStateFilter = entityStateFilterFactory.newFilter(
                false, false);

        assertTrue(entityStateFilter.filterDemand(baseAggregate));
    }
}

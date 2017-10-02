package com.vmturbo.repository.search;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unchecked")
public class AQLReprFuserTest {
    @Test
    public void testFusing() {
        final Filter<PropertyFilterType> entityTypeFilter = Filter.stringPropertyFilter("entityType", Filter.StringOperator.REGEX, "DataCenter");
        final Filter<PropertyFilterType> stateFilter = Filter.stringPropertyFilter("state", Filter.StringOperator.REGEX, "RUNNING");
        final Filter<TraversalFilterType> traversalHopFilter = Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 1);
        final Filter<PropertyFilterType> displayNameFilter = Filter.stringPropertyFilter("displayName", Filter.StringOperator.REGEX, "20");
        final Filter<PropertyFilterType> capacityFilter = Filter.numericPropertyFilter("capacity", Filter.NumericOperator.GTE, 2L);

        final AQLRepr repr1 = AQLRepr.fromFilters(entityTypeFilter);
        final AQLRepr repr2 = AQLRepr.fromFilters(stateFilter);
        final AQLRepr repr3 = AQLRepr.fromFilters(traversalHopFilter);
        final AQLRepr repr4 = AQLRepr.fromFilters(displayNameFilter);
        final AQLRepr repr5 = AQLRepr.fromFilters(capacityFilter);

        final ArrayList<AQLRepr> aqlReprs = Lists.newArrayList(repr1, repr2, repr3, repr4, repr5);

        final List<AQLRepr> fused = AQLReprFuser.fuse(aqlReprs);

        assertThat(fused).hasSize(2)
                .containsExactly(AQLRepr.fromFilters(entityTypeFilter, stateFilter),
                                 AQLRepr.fromFilters(traversalHopFilter, displayNameFilter, capacityFilter));
    }

    /**
     * Sometimes fusing is not possible.
     */
    @Test
    public void noFusing() {
        final Filter<PropertyFilterType> entityTypeFilter = Filter.stringPropertyFilter("entityType", Filter.StringOperator.REGEX, "DataCenter");
        final Filter<PropertyFilterType> stateFilter = Filter.stringPropertyFilter("state", Filter.StringOperator.REGEX, "RUNNING");
        final Filter<TraversalFilterType> traversalHopFilter = Filter.traversalHopFilter(Filter.TraversalDirection.CONSUMER, 1);
        final Filter<TraversalFilterType> traversalCondFilter = Filter.traversalCondFilter(Filter.TraversalDirection.PROVIDER, entityTypeFilter);

        final AQLRepr repr1 = AQLRepr.fromFilters(stateFilter);
        final AQLRepr repr2 = AQLRepr.fromFilters(traversalHopFilter);
        final AQLRepr repr3 = AQLRepr.fromFilters(traversalCondFilter);

        final ArrayList<AQLRepr> aqlReprs = Lists.newArrayList(repr1, repr2, repr3);

        final List<AQLRepr> fused = AQLReprFuser.fuse(aqlReprs);

        assertThat(fused).hasSize(3)
                .containsExactly(
                        AQLRepr.fromFilters(stateFilter),
                        AQLRepr.fromFilters(traversalHopFilter),
                        AQLRepr.fromFilters(traversalCondFilter));
    }
}
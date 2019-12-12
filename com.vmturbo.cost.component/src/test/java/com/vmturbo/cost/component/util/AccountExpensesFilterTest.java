package com.vmturbo.cost.component.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.util.AccountExpensesFilter.AccountExpenseFilterBuilder;

/**
 * Tests the {@link AccountExpensesFilter} class.
 */
public class AccountExpensesFilterTest {

    /**
     * This method implements methods {@code equals}, {@code hashCode}, {@code toString} that
     * has been overridden in this class.
     */
    @Test
    public void testObjectOverrideMethods() {
        AccountExpensesFilter filter = AccountExpenseFilterBuilder
            .newBuilder(TimeFrameCalculator.TimeFrame.LATEST)
            .duration(1L, 2L)
            .entityIds(Collections.singleton(5L))
            .entityTypes(ImmutableSet.of(10, 11))
            .accountIds(Collections.singleton(8L)).build();
        filter.toString();

        assertThat(filter.getStartDateMillis(), is(Optional.of(1L)));
        assertThat(filter.getEndDateMillis(), is(Optional.of(2L)));

        AccountExpenseFilterBuilder builder =
            AccountExpenseFilterBuilder.newBuilder(TimeFrameCalculator.TimeFrame.LATEST)
                .duration(1L, 2L)
                .entityIds(Collections.singleton(5L))
                .entityTypes(ImmutableSet.of(10, 11))
                .accountIds(Collections.singleton(8L));
        assertTrue(filter.equals(builder.build()));
        assertThat(filter.hashCode(), is(builder.build().hashCode()));
        assertFalse(filter.equals(null));
        builder.accountIds(Collections.singleton(9L));
        assertFalse(filter.equals(builder.build()));
        assertThat(filter.hashCode(),  not(builder.build()));
    }
}

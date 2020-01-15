package com.vmturbo.cost.component.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import org.jooq.Table;
import org.junit.Test;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.cost.component.db.Tables;
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
            .newBuilder(TimeFrame.LATEST)
            .duration(1L, 2L)
            .entityIds(Collections.singleton(5L))
            .entityTypes(ImmutableSet.of(10, 11))
            .accountIds(Collections.singleton(8L)).build();
        filter.toString();

        assertThat(filter.getStartDateMillis(), is(Optional.of(1L)));
        assertThat(filter.getEndDateMillis(), is(Optional.of(2L)));

        AccountExpenseFilterBuilder builder =
            AccountExpenseFilterBuilder.newBuilder(TimeFrame.LATEST)
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

    /**
     * Test that when in "Latest" time frame, the account_expenses table is queried.
     */
    @Test
    public void testGetTableLatestTimeframe() {
        AccountExpensesFilter accountExpensesFilter = createAccountExpenseFilter(TimeFrame.LATEST);

        verifyTable(Tables.ACCOUNT_EXPENSES, accountExpensesFilter);
    }

    /**
     * Test that when in "Day" time frame, the account_expenses table is queried.
     */
    @Test
    public void testGetTableDayTimeframe() {
        AccountExpensesFilter accountExpensesFilter = createAccountExpenseFilter(TimeFrame.DAY);

        verifyTable(Tables.ACCOUNT_EXPENSES, accountExpensesFilter);
    }

    /**
     * Test that when in "Month" time frame, the account_expenses_by_month table is queried.
     */
    @Test
    public void testGetTableMonthTimeframe() {
        AccountExpensesFilter accountExpensesFilter = createAccountExpenseFilter(TimeFrame.MONTH);

        verifyTable(Tables.ACCOUNT_EXPENSES_BY_MONTH, accountExpensesFilter);
    }

    /**
     * Test that when in "Hour" time frame, the account_expenses table is queried, and an error is
     * printed to the log.
     */
    @Test
    public void testGetTableInvalidTimeframe() {
        AccountExpensesFilter accountExpensesFilter = createAccountExpenseFilter(TimeFrame.HOUR);

        verifyTable(Tables.ACCOUNT_EXPENSES, accountExpensesFilter);
    }

    private AccountExpensesFilter createAccountExpenseFilter(final TimeFrame timeFrame) {
        return AccountExpenseFilterBuilder.newBuilder(timeFrame).build();
    }

    private void verifyTable(final Table<?> expectedTable,
                             final AccountExpensesFilter accountExpensesFilter) {
        assertEquals(expectedTable, accountExpensesFilter.getTable());
    }

}

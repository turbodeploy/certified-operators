package com.vmturbo.cost.component.expenses;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.cost.component.util.CostFilter;
import com.vmturbo.sql.utils.DbException;

/**
 * This class is used to manage the account expenses table. Currently billing probe will discover
 * expenses twice a day. So it will add two entries every day.
 * TODO: delete the old entries based on retention periods if needed.
 */
public interface AccountExpensesStore {
    /**
     * Persist a account expense, based on AccountExpensesInfo {@link AccountExpensesInfo}.
     * @param associatedAccountId the OID of the business account
     * @param usageDate usage date for the account expenses in epoch milliseconds
     * @param accountExpensesInfo the expenses information
     * @throws DbException if anything goes wrong in the database
     */
    @Nonnull
    void persistAccountExpenses(
            long associatedAccountId,
            long usageDate,
            @Nonnull final AccountExpensesInfo accountExpensesInfo) throws DbException;

    /**
     * Returns all the existing account expenses.
     *
     * @return set of existing account expenses.
     * @throws DbException if anything goes wrong in the database
     */
    @Nonnull
    List<Cost.AccountExpenses> getAllAccountExpenses() throws DbException;

    /**
     * Delete account expense by associated account id.
     *
     * @param associatedAccountId associated account id
     * @throws AccountExpenseNotFoundException if the account expense with associated account id doesn't exist
     * @throws DbException                     if anything goes wrong in the database
     */
    void deleteAccountExpensesByAssociatedAccountId(final long associatedAccountId) throws AccountExpenseNotFoundException, DbException;

    /**
     * Get account expenses based on the the account expense filter .
     * It returns Map with entry (timestamp -> (associatedAccountId -> AccountExpenses)).
     * In a timestamp/snapshot, the account expenses with same ids will be combined to one account expense.
     *
     * @param filter account expenses filter which has resolved start and end dates and appropriated tables
     * @return Map with entry (timestamp -> (associatedAccountId -> AccountExpenses))
     * @throws DbException if anything goes wrong in the database
     */
    @Nonnull
    Map<Long, Map<Long, Cost.AccountExpenses>> getAccountExpenses(@Nonnull final CostFilter filter) throws DbException;
}
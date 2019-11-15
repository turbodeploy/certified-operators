package com.vmturbo.cost.component.expenses;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest.AccountExpenseQueryScope;
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
     * @return discount object, if created
     * @throws DbException if anything goes wrong in the database
     */
    @Nonnull
    void persistAccountExpenses(
            final long associatedAccountId,
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
     * Get account expenses by associated account id.
     *
     * @param queryScope Describes which accounts to return expenses for.
     * @return Collection of account expenses match the associated account id
     * @throws DbException if anything goes wrong in the database
     */
    @Nonnull
    Collection<AccountExpenses> getCurrentAccountExpenses(AccountExpenseQueryScope queryScope) throws DbException;

    /**
     * Get the latest account expenses.
     * It returns Map with entry (timestamp -> (associatedAccountId -> AccountExpenses)).
     * In a timestamp/snapshot, the account expenses with same ids will be combined to one account expense.
     * @Param entityIds entity ids
     * @Param entityTypeIds entity type ids
     * @return Map with entry (timestamp -> (associatedAccountId -> AccountExpenses))
     * @throws DbException if anything goes wrong in the database
     */
    @Nonnull
    Map<Long, Map<Long, Cost.AccountExpenses>> getLatestExpenses(@Nonnull final Set<Long> entityIds,
                                                                 @Nonnull final Set<Integer> entityTypeIds) throws DbException;

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
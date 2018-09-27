package com.vmturbo.cost.component.expenses;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.sql.utils.DbException;

/**
 * This class is used to manage the account expenses table. Currently billing probe will discover
 * expenses twice a day. So it will add two entries every day.
 * TODO: delete the old entries based on retention periods if needed.
 */
public interface AccountExpensesStore {
    /**
     * Persist a account expense, based on AccountExpensesInfo {@link AccountExpensesInfo}.
     *
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
     * @param associatedAccountId associated account id
     * @return set of account expenses match the associated account id
     * @throws DbException if anything goes wrong in the database
     */
    @Nonnull
    List<Cost.AccountExpenses> getAccountExpensesByAssociatedAccountId(final long associatedAccountId) throws DbException;

    /**
     * Get account expenses between the start (minValue) and end (maxValue) dates.
     * It returns Map with entry (timestamp -> (associatedAccountId -> AccountExpenses)).
     * In a timestamp/snapshot, the account expenses with same ids will be combined to one account expense.
     *
     * @param startDate start date
     * @param endDate   end date
     * @return Map with entry (timestamp -> (associatedAccountId -> AccountExpenses))
     * @throws DbException if anything goes wrong in the database
     */
    Map<Long, Map<Long, AccountExpenses>> getAccountExpenses(@Nonnull final LocalDateTime startDate,
                                                             @Nonnull final LocalDateTime endDate) throws DbException;

    /**
     * Get the latest account expenses.
     * It returns Map with entry (timestamp -> (associatedAccountId -> AccountExpenses)).
     * In a timestamp/snapshot, the account expenses with same ids will be combined to one account expense.
     *
     * @return Map with entry (timestamp -> (associatedAccountId -> AccountExpenses))
     * @throws DbException if anything goes wrong in the database
     */
    Map<Long, Map<Long, Cost.AccountExpenses>> getAccountLatestExpenses() throws DbException;

        /**
     * Delete account expense by associated account id.
     *
     * @param associatedAccountId associated account id
     * @throws AccountExpenseNotFoundException if the account expense with associated account id doesn't exist
     * @throws DbException                     if anything goes wrong in the database
     */
    void deleteAccountExpensesByAssociatedAccountId(final long associatedAccountId) throws AccountExpenseNotFoundException, DbException;
}
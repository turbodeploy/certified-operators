package com.vmturbo.cost.component.expenses;

import static com.vmturbo.cost.component.db.tables.AccountExpenses.ACCOUNT_EXPENSES;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.cost.component.db.tables.pojos.AccountExpenses;
import com.vmturbo.cost.component.db.tables.records.AccountExpensesRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * {@inheritDoc}
 */
public class SqlAccountExpensesStore implements AccountExpensesStore {

    private final DSLContext dsl;

    public SqlAccountExpensesStore(@Nonnull final DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Cost.AccountExpenses persistAccountExpenses(final long associatedAccountId,
                                                       @Nonnull final AccountExpensesInfo accountExpensesInfo)
            throws DbException {
        Objects.requireNonNull(accountExpensesInfo);
        final LocalDateTime curTime = LocalDateTime.now();
        try {
            AccountExpenses accountExpenses = new AccountExpenses(associatedAccountId, accountExpensesInfo, curTime);
            AccountExpensesRecord accountExpensesRecord = dsl.newRecord(ACCOUNT_EXPENSES, accountExpenses);
            accountExpensesRecord.store();
            return toDTO(accountExpensesRecord);
        } catch (DataAccessException dataAccessException) {
            throw new DbException(dataAccessException.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateAccountExpenses(final long associatedAccountId,
                                      @Nonnull final AccountExpensesInfo accountExpensesInfo)
            throws AccountExpenseNotFoundException, DbException {
        try {
            Objects.requireNonNull(accountExpensesInfo);
            if (dsl.update(ACCOUNT_EXPENSES)
                    .set(ACCOUNT_EXPENSES.ACCOUNT_EXPENSES_INFO, accountExpensesInfo)
                    .where(ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID.eq(associatedAccountId))
                    .execute() == 0) {
                throw new AccountExpenseNotFoundException("Account association id " + associatedAccountId +
                        " is not found. Could not update account expenses");
            }
        } catch (DataAccessException dataAccessException) {
            throw new DbException(dataAccessException.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<Cost.AccountExpenses> getAllAccountExpenses() throws DbException {
        try {
            return dsl.selectFrom(ACCOUNT_EXPENSES).fetch().map(this::toDTO);
        } catch (DataAccessException dataAccessException) {
            throw new DbException(dataAccessException.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<Cost.AccountExpenses> getAccountExpensesByAssociatedAccountId(final long associatedAccountId)
            throws DbException {
        try {
            return dsl.selectFrom(ACCOUNT_EXPENSES)
                    .where(ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID.eq(associatedAccountId))
                    .fetch()
                    .map(this::toDTO);
        } catch (DataAccessException dataAccessException) {
            throw new DbException(dataAccessException.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAccountExpensesByAssociatedAccountId(final long associatedAccountId)
            throws AccountExpenseNotFoundException, DbException {
        try {
            if (dsl.deleteFrom(ACCOUNT_EXPENSES)
                    .where(ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID.eq(associatedAccountId))
                    .execute() == 0) {
                throw new AccountExpenseNotFoundException("Account association id " + associatedAccountId +
                        " is not found. Could not delete account expenses");
            }
        } catch (DataAccessException dataAccessException) {
            throw new DbException(dataAccessException.getMessage());
        }
    }

    //Convert accountExpensesRecord DB record to account expenses proto DTO
    private Cost.AccountExpenses toDTO(@Nonnull final AccountExpensesRecord accountExpensesRecord) {
        return Cost
                .AccountExpenses
                .newBuilder()
                .setAssociatedAccountId(accountExpensesRecord.getAssociatedAccountId())
                .setAccountExpensesInfo(accountExpensesRecord.getAccountExpensesInfo())
                .build();
    }
}

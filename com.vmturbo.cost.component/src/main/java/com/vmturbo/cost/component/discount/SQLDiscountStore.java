package com.vmturbo.cost.component.discount;

import static com.vmturbo.cost.component.db.Tables.DISCOUNT;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.cost.component.db.tables.pojos.Discount;
import com.vmturbo.cost.component.db.tables.records.DiscountRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * {@inheritDoc}
 */
public class SQLDiscountStore implements DiscountStore {

    private final IdentityProvider identityProvider;

    private final DSLContext dsl;

    public SQLDiscountStore(@Nonnull final DSLContext dsl,
                            @Nonnull final IdentityProvider identityProvider) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.dsl = Objects.requireNonNull(dsl);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Cost.Discount persistDiscount(final long associatedAccountId,
                                         @Nonnull final DiscountInfo discountInfo)
            throws DuplicateAccountIdException, DbException {
        try {
            return dsl.transactionResult(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                final DiscountRecord previousDiscount = transactionDsl.selectFrom(DISCOUNT)
                        .where(DISCOUNT.ASSOCIATED_ACCOUNT_ID.eq(associatedAccountId))
                        .fetchOne();
                if (previousDiscount != null) {
                    throw new DuplicateAccountIdException("Found duplicate account id "
                            + associatedAccountId + ", could not save.");
                }
                Discount discount = new Discount(identityProvider.next(), associatedAccountId, discountInfo);
                DiscountRecord discountRecord = transactionDsl.newRecord(DISCOUNT, discount);
                discountRecord.store();
                return toDTO(discountRecord);
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof DuplicateAccountIdException) {
                throw (DuplicateAccountIdException) e.getCause();
            } else {
                throw new DbException(e.getMessage());
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDiscount(final long id,
                               @Nonnull final DiscountInfo discountInfo)
            throws DiscountNotFoundException, DbException {
        try {
            if (dsl.update(DISCOUNT)
                    .set(DISCOUNT.DISCOUNT_INFO, discountInfo)
                    .where(DISCOUNT.ID.eq(id))
                    .execute() != 1) {
                throw new DiscountNotFoundException("Discount id " + id +
                        " is not found. Could not update");
            }
        } catch (DataAccessException e) {
            throw new DbException(e.getMessage());
        }
    }

    @Override
    public void updateDiscountByAssociatedAccount(final long associatedAccountId, @Nonnull final DiscountInfo discountInfo) throws DiscountNotFoundException, DbException {
        try {
            if (dsl.update(DISCOUNT)
                    .set(DISCOUNT.DISCOUNT_INFO, discountInfo)
                    .where(DISCOUNT.ASSOCIATED_ACCOUNT_ID.eq(associatedAccountId))
                    .execute() != 1) {
                throw new DiscountNotFoundException("Associated account id " + associatedAccountId +
                        " is not found. Could not update");
            }
        } catch (DataAccessException e) {
            throw new DbException(e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<Cost.Discount> getAllDiscount() throws DbException {

        try {
            return dsl.selectFrom(DISCOUNT)
                    .fetch()
                    .map(this::toDTO);
        } catch (DataAccessException e) {
            throw new DbException(e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<Cost.Discount> getDiscountByDiscountId(final long id) throws DbException {
        try {
            return dsl.selectFrom(DISCOUNT)
                    .where(DISCOUNT.ID.eq(id))
                    .fetch()
                    .map(this::toDTO);
        } catch (DataAccessException e) {
            throw new DbException(e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<Cost.Discount> getDiscountByAssociatedAccountId(final long associatedAccountId)
            throws DbException {
        try {
            return dsl.selectFrom(DISCOUNT)
                    .where(DISCOUNT.ASSOCIATED_ACCOUNT_ID.eq(associatedAccountId))
                    .fetch()
                    .map(this::toDTO);
        } catch (DataAccessException e) {
            throw new DbException(e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDiscountByDiscountId(final long id) throws DiscountNotFoundException, DbException {
        try {
            if (dsl.deleteFrom(DISCOUNT).where(DISCOUNT.ID.eq(id))
                    .execute() != 1) {
                throw new DiscountNotFoundException("Discount id " + id +
                        " is not found. Could not delete");
            }
        } catch (DataAccessException e) {
            throw new DbException(e.getMessage());
        }
    }

    @Override
    public void deleteDiscountByAssociatedAccountId(final long id) throws DiscountNotFoundException, DbException {
        try {
            if (dsl.deleteFrom(DISCOUNT).where(DISCOUNT.ASSOCIATED_ACCOUNT_ID.eq(id))
                    .execute() != 1) {
                throw new DiscountNotFoundException("Discount id " + id +
                        " is not found. Could not delete");
            }
        } catch (DataAccessException e) {
            throw new DbException(e.getMessage());
        }

    }

    //Convert discount DB record to discount proto DTO
    private Cost.Discount toDTO(@Nonnull final DiscountRecord discount) {
        return Cost.Discount.newBuilder()
                .setAssociatedAccountId(discount.getAssociatedAccountId())
                .setDiscountInfo(discount.getDiscountInfo())
                .setId(discount.getId())
                .build();
    }
}

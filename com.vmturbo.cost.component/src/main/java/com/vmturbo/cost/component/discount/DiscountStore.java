package com.vmturbo.cost.component.discount;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.sql.utils.DbException;

/**
 * This class is used to manage the business discount table.
 */
public interface DiscountStore {
    /**
     * Persist a discount, based on DiscountInfo {@link DiscountInfo}.
     * Note: @param associatedAccountId has UNIQUE constraint on DB schema,
     * see V1_2__create_discount.sql
     *
     * @return discount object, if created
     * @throws DuplicateAccountIdException if the discount with discount id already exists
     * @throws DbException if anything goes wrong in the database
     */
    @Nonnull
    Cost.Discount persistDiscount(
            final long associatedAccountId,
            @Nonnull final DiscountInfo discountInfo) throws DuplicateAccountIdException, DbException;

    /**
     * Update discount by discount id.
     *
     * @param id           discount id
     * @param discountInfo discount Info proto ojbject
     * @throws DiscountNotFoundException if the discount with discount id doesn't exist
     * @throws DbException               if anything goes wrong in the database
     */
    void updateDiscount(
            final long id,
            @Nonnull final DiscountInfo discountInfo) throws DiscountNotFoundException, DbException;

    /**
     * Update discount by discount id.
     *
     * @param id           discount id
     * @param discountInfo discount Info proto ojbject
     * @throws DiscountNotFoundException if the discount with discount id doesn't exist
     * @throws DbException               if anything goes wrong in the database
     */
    void updateDiscountByAssociatedAccount(
            final long associatedAccountId,
            @Nonnull final DiscountInfo discountInfo) throws DiscountNotFoundException, DbException;


    /**
     * Returns all the existing discounts.
     *
     * @return set of existing discounts.
     * @throws DbException if anything goes wrong in the database
     */
    @Nonnull
    List<Discount> getAllDiscount() throws DbException;

    /**
     * Get discount by id.
     *
     * @param id discount id
     * @return set of discounts match the discount id
     * @throws DbException if anything goes wrong in the database
     */
    @Nonnull
    List<Cost.Discount> getDiscountByDiscountId(final long id) throws DbException;

    /**
     * Get discount by associated account id.
     *
     * @param associatedAccountId associated account id
     * @return set of discounts match the associated account id
     * @throws DbException if anything goes wrong in the database
     */
    @Nonnull
    List<Cost.Discount> getDiscountByAssociatedAccountId(final long associatedAccountId) throws DbException;

    /**
     * Delete discount by discount id.
     *
     * @param id discount id
     * @throws DiscountNotFoundException if the discount with discount id doesn't exist
     * @throws DbException               if anything goes wrong in the database
     */
    void deleteDiscountByDiscountId(final long id) throws DiscountNotFoundException, DbException;

    /**
     * Delete discount by discount id.
     *
     * @param id discount id
     * @throws DiscountNotFoundException if the discount with discount id doesn't exist
     * @throws DbException               if anything goes wrong in the database
     */
    void deleteDiscountByAssociatedAccountId(final long id) throws DiscountNotFoundException, DbException;
}
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
     */
    @Nonnull
    Cost.Discount persistDiscount(
            final long associatedAccountId,
            @Nonnull final DiscountInfo discountInfo) throws DuplicateAccountIdException;

    /**
     * Update discount by discount id.
     *
     * @param id           discount id
     * @param discountInfo discount Info proto ojbject
     * @throws DbException if the discount with discount id doesn't exist
     */
    void updateDiscount(
            final long id,
            @Nonnull final DiscountInfo discountInfo) throws DiscountNotFoundException;


    /**
     * Returns all the existing discounts.
     *
     * @return set of existing discounts.
     */
    @Nonnull
    List<Discount> getAllDiscount();

    /**
     * Get discount by id.
     *
     * @param id discount id
     * @return set of discounts match the discount id
     */
    @Nonnull
    List<Cost.Discount> getDiscountByDiscountId(final long id);

    /**
     * Get discount by associated account id.
     *
     * @param associatedAccountId associated account id
     * @return set of discounts match the associated account id
     */
    @Nonnull
    List<Cost.Discount> getDiscountByAssociatedAccountId(final long associatedAccountId);

    /**
     * Delete discount by discount id.
     *
     * @param id discount id
     * @throws DbException if the discount with discount id doesn't exist
     */
    void deleteDiscountByDiscountId(final long id) throws DiscountNotFoundException;
}
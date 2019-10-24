package com.vmturbo.cost.calculation;

import static com.vmturbo.trax.Trax.trax;
import static com.vmturbo.trax.Trax.traxConstant;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.ServiceLevelDiscount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.TierLevelDiscount;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.trax.TraxNumber;

/**
 * The discount applicator is responsible for calculating the discount that applies to
 * a particular (entity, provider) pair.
 *
 * A single {@link DiscountApplicator} is associated with a single entity. Each entity will
 * have at most one {@link Discount} associated with it - found by walking up the chain of
 * business accounts that own the entity to the master account. Once you have a
 * {@link DiscountApplicator}, you calculate the discount to apply to the price of any particular
 * provider by calling {@link DiscountApplicator#getDiscountPercentage(long)} or
 * {@link DiscountApplicator#getDiscountPercentage(Object)}.
 *
 * Note: At the time of this writing only master accounts can have {@link Discount}s, and only
 * one master account will be associated with any given entity. This means we don't need to worry
 * about overlaps between {@link Discount}s.
 *
 * @param <ENTITY_CLASS> The class used to represent entities in the topology. For example,
 *                      {@link TopologyEntityDTO} for the realtime topology.
 */
public class DiscountApplicator<ENTITY_CLASS> {

    /**
     * Static instance to represent an applicator for no discount.
     */
    private static final DiscountApplicator EMPTY_APPLICATOR =
            new DiscountApplicator(null, null, null, null);

    /**
     * A constant {@link TraxNumber} representing no discount.
     */
    public static final TraxNumber NO_DISCOUNT = traxConstant(0, "no discount");

    /**
     * The entity this {@link DiscountApplicator} is associated with. Mostly for debugging purposes,
     * since we don't need the entity itself to calculate the discount.
     */
    private final ENTITY_CLASS entity;

    /**
     * The discount associated with the entity.
     */
    private final Discount discount;

    /**
     * The topology the entity resides in. We need the topology because we need to make
     * some traversals to find the right discount (e.g. find the service associated with a tier).
     */
    private final CloudTopology<ENTITY_CLASS> cloudTopology;

    /**
     * The extractor for the entity class.
     */
    private final EntityInfoExtractor<ENTITY_CLASS> infoExtractor;

    /**
     * Use {@link DiscountApplicator#newFactory()} to obtain a factory to create applicators.
     */
    private DiscountApplicator(@Nullable final ENTITY_CLASS entity,
                               @Nullable final Discount discount,
                               @Nullable final CloudTopology<ENTITY_CLASS> cloudTopology,
                               @Nullable final EntityInfoExtractor<ENTITY_CLASS> infoExtractor) {
        this.entity = entity;
        this.discount = discount;
        this.cloudTopology = cloudTopology;
        this.infoExtractor = infoExtractor;
    }

    @VisibleForTesting
    @Nullable
    public Discount getDiscount() {
        return discount;
    }

    /**
     * Return an applicator for "no discount".
     */
    public static <ENTITY_CLASS> DiscountApplicator<ENTITY_CLASS> noDiscount() {
        return (DiscountApplicator<ENTITY_CLASS>)EMPTY_APPLICATOR;
    }

    /**
     * See {@link DiscountApplicator#getDiscountPercentage(long)}.
     *
     * @param provider The provider of a particular commodity.
     * @return See {@link DiscountApplicator#getDiscountPercentage(long)}.
     */
    public TraxNumber getDiscountPercentage(@Nonnull final ENTITY_CLASS provider) {
        return infoExtractor == null ? NO_DISCOUNT : getDiscountPercentage(infoExtractor.getId(provider));
    }

    /**
     * Get the discount percentage to apply to the prices of commodities sold by a particular
     * provider to the entity this {@link DiscountApplicator} is associated with.
     *
     * @param providerId The provider of a particular commodity.
     * @return A number between 0 and 1 indicating the amount of the discount. For example, a 20%
     *         discount would be represented by 0.2.
     */
    public TraxNumber getDiscountPercentage(final long providerId) {
        if (discount == null || infoExtractor == null || cloudTopology == null) {
            return NO_DISCOUNT;
        }

        final DiscountInfo discountInfo = discount.getDiscountInfo();

        // If multiple levels of discounts apply to the provider we apply the "closest" discount,
        // not the greatest discount. The discounts don't stack. (i.e. if there is a 20%
        // discount for the tier and a 30% account discount, the total discount is 20%).

        final TierLevelDiscount tierLevelDiscount =
                discountInfo.getTierLevelDiscount();
        final Double tierDiscount = tierLevelDiscount.getDiscountPercentageByTierIdMap().get(providerId);
        if (tierDiscount != null) {
            return trax(tierDiscount, "Tier discount");
        }

        final ServiceLevelDiscount serviceLevelDiscount = discountInfo.getServiceLevelDiscount();
        final Double serviceDiscount = cloudTopology.getConnectedService(providerId)
                .map(infoExtractor::getId)
                .map(serviceId -> serviceLevelDiscount.getDiscountPercentageByServiceIdMap().get(serviceId))
                .orElse(null);
        if (serviceDiscount != null) {
            return trax(serviceDiscount, "Service discount");
        }

        if (discountInfo.getAccountLevelDiscount().hasDiscountPercentage()) {
            return trax(discountInfo.getAccountLevelDiscount().getDiscountPercentage(), "Account discount");
        } else {
            return NO_DISCOUNT;
        }
    }

    @Override
    public String toString() {
        if (this == EMPTY_APPLICATOR || discount == null) {
            return "No Discount";
        }
        return TextFormat.printToString(discount);
    }

    /**
     * Create a factory for {@link DiscountApplicator} instances.
     *
     * @param <ENTITY_CLASS> See {@link DiscountApplicator}.
     * @return A {@link DiscountApplicatorFactory} to construct {@link DiscountApplicator}s.
     */
    public static <ENTITY_CLASS> DiscountApplicatorFactory<ENTITY_CLASS> newFactory() {
        return new DefaultDiscountApplicatorFactory<>();
    }

    /**
     * A factory to construct {@link DiscountApplicator}s. Get an instance of it via
     * {@link DiscountApplicator#newFactory()}.
     *
     * @param <ENTITY_CLASS> See {@link DiscountApplicator}.
     */
    public interface DiscountApplicatorFactory<ENTITY_CLASS> {

        /**
         * Get the {@link DiscountApplicator} for a particular entity in the topology.
         *
         * @param entity The entity.
         * @param cloudTopology The {@link CloudTopology} the entity is in.
         * @param infoExtractor The {@link EntityInfoExtractor} to extract information from entities
         *                      in the topology.
         * @param cloudCostData The {@link CloudCostData} containing, among other things, the discounts.
         * @return A {@link DiscountApplicator} for the entity.
         */
        @Nonnull
        DiscountApplicator<ENTITY_CLASS> entityDiscountApplicator(@Nonnull final ENTITY_CLASS entity,
                                          @Nonnull final CloudTopology<ENTITY_CLASS> cloudTopology,
                                          @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
                                          @Nonnull final CloudCostData cloudCostData);

        /**
         * Get an applicator for a particular business account. This is a utility method - it's
         * the equivalent of getting the entity associated with the account, and calling
         * {@link DiscountApplicatorFactory#entityDiscountApplicator(Object, CloudTopology, EntityInfoExtractor, CloudCostData)}.
         */
        @Nonnull
        DiscountApplicator<ENTITY_CLASS> accountDiscountApplicator(final long accountId,
                                          @Nonnull final CloudTopology<ENTITY_CLASS> cloudTopology,
                                          @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
                                          @Nonnull final CloudCostData cloudCostData);
    }


    /**
     * The default implementation of {@link DiscountApplicatorFactory}. Do not construct directly -
     * use {@link DiscountApplicator#newFactory()}.
     */
    private static class DefaultDiscountApplicatorFactory<ENTITY_CLASS> implements DiscountApplicatorFactory<ENTITY_CLASS> {

        private static final Logger logger = LogManager.getLogger();

        private DefaultDiscountApplicatorFactory() {}

        /**
         * {@inheritDoc}
         */
        @Override
        @Nonnull
        public DiscountApplicator<ENTITY_CLASS> entityDiscountApplicator(@Nonnull final ENTITY_CLASS entity,
                     @Nonnull final CloudTopology<ENTITY_CLASS> cloudTopology,
                     @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
                     @Nonnull final CloudCostData cloudCostData) {
            Optional<Discount> discountOpt;
            Optional<ENTITY_CLASS> lastOwnerOpt = Optional.of(entity);
            // Walk up the business accounts owning the entity until there are no more owners,
            // or until we find a discount.
            do {
                lastOwnerOpt = lastOwnerOpt.flatMap(lastOwner -> {
                    final long lastOwnerId = infoExtractor.getId(lastOwner);
                    return cloudTopology.getOwner(lastOwnerId)
                        .filter(newOwner -> {
                            final int newOwnerType = infoExtractor.getEntityType(newOwner);
                            if (newOwnerType != EntityType.BUSINESS_ACCOUNT_VALUE) {
                                logger.warn("Entity {} has unexpected owner type {}", lastOwnerId, newOwnerType);
                                return false;
                            } else {
                                return true;
                            }
                        });
                });

                discountOpt = lastOwnerOpt.flatMap(owner ->
                        cloudCostData.getDiscountForAccount(infoExtractor.getId(owner)));
            } while (lastOwnerOpt.isPresent() && !discountOpt.isPresent());

            return discountOpt.map(discount -> new DiscountApplicator<>(entity, discount, cloudTopology, infoExtractor))
                .orElseGet(DiscountApplicator::noDiscount);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Nonnull
        public DiscountApplicator<ENTITY_CLASS> accountDiscountApplicator(final long accountId,
                      @Nonnull final CloudTopology<ENTITY_CLASS> cloudTopology,
                      @Nonnull final EntityInfoExtractor<ENTITY_CLASS> infoExtractor,
                      @Nonnull final CloudCostData cloudCostData) {
            return cloudTopology.getEntity(accountId)
                .map(accountEntity -> entityDiscountApplicator(accountEntity, cloudTopology, infoExtractor, cloudCostData))
                .orElseGet(DiscountApplicator::noDiscount);
        }
    }
}

package com.vmturbo.cost.calculation.journal.entry;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.CostJournal.RateExtractor;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxNumber;

/**
 * A {@link QualifiedJournalEntry} for on-demand payments to entities in the topology.
 *
 * @param <E> see {@link QualifiedJournalEntry}
 */
@Immutable
public class OnDemandJournalEntry<E> implements QualifiedJournalEntry<E> {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The payee - i.e. the entity that's selling the item to the buyer.
     */
    private final E payee;

    /**
     * The unit price at which the payee is selling whatever item the {@link QualifiedJournalEntry}
     * represents.
     */
    private final Price price;

    /**
     * The number of units of the item that the buyer is buying from the payee. This can
     * be combined with the price to get the cost of the item to the buyer.
     */
    private final TraxNumber unitsBought;

    private final Optional<CostSource> costSource;

    private final CostCategory costCategory;

    /**
     * Constructor.
     * @param payee the payee
     * @param price the price at which the entity is purchasing from the payee
     * @param unitsBought the number of units of the item that the buyer is buying from the payee
     * @param costCategory the cost category
     * @param costSource the cost source
     */
    public OnDemandJournalEntry(
            @Nonnull final E payee,
            @Nonnull final Price price,
            @Nonnull final TraxNumber unitsBought,
            @Nonnull final CostCategory costCategory,
            @Nonnull final Optional<CostSource> costSource) {
        Preconditions.checkArgument(unitsBought.getValue() >= 0);
        this.payee = payee;
        this.price = price;
        this.unitsBought = unitsBought;
        this.costCategory = costCategory;
        this.costSource = costSource;
    }

    @Override
    public TraxNumber calculateHourlyCost(
            @Nonnull final EntityInfoExtractor<E> infoExtractor,
            @Nonnull final DiscountApplicator<E> discountApplicator,
            @Nonnull final RateExtractor rateExtractor) {
        logger.trace("Calculating hourly cost for purchase from entity {} of type {}",
                infoExtractor.getId(payee), infoExtractor.getEntityType(payee));
        final TraxNumber unitPrice = Trax.trax(price.getPriceAmount().getAmount(),
                infoExtractor.getName(payee) + " unit price");
        final TraxNumber discountPercentage = Trax.trax(1.0, "full price portion")
                .minus(discountApplicator.getDiscountPercentage(payee))
                .compute("discount coefficient");
        final TraxNumber discountedUnitPrice =
                unitPrice.times(discountPercentage).compute("discounted unit price");
        final TraxNumber totalPrice = discountedUnitPrice.times(unitsBought).compute("total price");
        logger.trace("Buying {} {} at unit price {} with discount percentage {}", unitsBought,
                price.getUnit(), unitPrice, discountPercentage);
        final TraxNumber cost;
        switch (price.getUnit()) {
            case HOURS:
                cost = totalPrice;
                break;
            case DAYS:
                cost = totalPrice.dividedBy(CostProtoUtil.HOURS_IN_DAY, "hrs in day")
                        .compute("hourly cost for " + infoExtractor.getName(payee));
                break;
            case MONTH:
            case MILLION_IOPS:
            case GB_MONTH:
                // In all of these cases, the key distinction is that the price is monthly,
                // so to get the hourly price we need to divide.
                cost = totalPrice.dividedBy(CostProtoUtil.HOURS_IN_MONTH, "hrs in month")
                        .compute("hourly cost for " + infoExtractor.getName(payee));
                break;
            default:
                logger.warn("Unsupported unit: {}", price.getUnit());
                cost = Trax.trax(0, "unsupported unit");
                break;
        }
        logger.trace("Purchase from entity {} of type {} has cost: {}", infoExtractor.getId(payee),
                infoExtractor.getEntityType(payee), cost);
        return cost;
    }

    @Nonnull
    @Override
    public Optional<CostSource> getCostSource() {
        return costSource;
    }

    @Nonnull
    @Override
    public CostCategory getCostCategory() {
        return costCategory;
    }

    @Override
    public int compareTo(final Object o) {
        return -1;
    }

    public E getPayee() {
        return payee;
    }

    public Price getPrice() {
        return price;
    }

    public TraxNumber getUnitsBought() {
        return unitsBought;
    }
}

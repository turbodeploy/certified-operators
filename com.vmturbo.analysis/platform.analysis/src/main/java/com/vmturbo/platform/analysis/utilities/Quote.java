package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.protobuf.BalanceAccountDTOs.BalanceAccountDTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CoverageEntry;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactoryHelper.CapacityLimitation;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactoryHelper.RatioBasedResourceDependency;

/**
 * A {@link Quote} is the output of running a {@link com.vmturbo.platform.analysis.pricefunction.QuoteFunction}
 * to attempt to see how expensive it would be to buy the {@link com.vmturbo.platform.analysis.economy.Basket}
 * of commodities required by a particular {@link ShoppingList} from a particular {@link Trader}.
 *
 * In the case when the value on a {@link Quote} is {@link Double#POSITIVE_INFINITY}, the {@link Quote}
 * may contain additional information that can be used to explain the cause of the infinite quote.
 *
 * For example, if a {@link ShoppingList} requires 1000 quantity of commodity "Foo", and trader X
 * only has 500 "Foo" available, we can create a {@link CommodityQuote} with information that says
 * the {@link Quote} for the {@link ShoppingList} on trader X is infinity because it only has 500 "Foo"
 * available when 1000 was requested.
 *
 * There are several specific {@link Quote} variants for use when the cause of an infinite {@link Quote}
 * may be due to something other than a simple insufficient commodity
 * (ie {@link InfiniteDependentComputeCommodityQuote}, etc.)
 *
 * The base {@link Quote} class to mutate its internal state, but subclasses of {@link Quote}
 * may expose methods for mutation.
 */
public abstract class Quote {

    protected static final Logger logger = LogManager.getLogger();

    /**
     * The maximum rank a quote is allowed to have. A quote should not return a rank
     * higher than the max rank.
     */
    public static int MAX_RANK = Integer.MAX_VALUE - 1;

    /**
     * An invalid rank indicating the quote is not valid.
     */
    public static int INVALID_RANK = MAX_RANK + 1;

    /**
     * An array containing the quote offered by the seller for the given shopping list,
     * or a part of it greater than or equal to bestQuoteSoFar iff the actual quote would
     * exceed that value or if any commodity returns an INFINITE price, the minQuote and
     * the maxQuote.
     *
     * The value at index 0 is the value.
     * The value at index 1 is the min.
     * The value at index 2 is the max.
     *
     * The value of a quote is commonly used, but the min and max are used less frequently.
     */
    protected double[] quoteValues = new double[3];

    protected boolean isQuoteComplete = true;
    /**
     * The seller associated with this {@link Quote}. A {@link Quote} is generated when
     * attempting to place a particular {@link ShoppingList} on a particular seller,
     * and this is the seller for this {@link Quote}.
     *
     * Note that it is possible to have a null seller. For example, when a {@link ShoppingList}
     * cannot be placed due to no sellers in the market that the {@link ShoppingList} would like
     * to buy from, we may generate an infinite {@link Quote} with no seller.
     */
    protected final Trader seller;

    protected Double moveCost = 0.0;

    /**
     * Create a new {@link Quote}.
     *
     * @param seller The seller associated with this {@link Quote}. May be null.
     */
    protected Quote(@Nullable final Trader seller) {
        this.seller = seller;
    }

    /**
     * Create a {@link Quote} with a particular value.
     *
     * @param seller The seller associated with this {@link Quote}. May be null.
     * @param quoteValue The value for the quote.
     * @param minQuoteValue The min value for the quote.
     * @param maxQuoteValue The max value for the quote.
     */
    protected Quote(@Nullable final Trader seller, final double quoteValue,
                 final double minQuoteValue, final double maxQuoteValue) {
        quoteValues[0] = quoteValue;
        quoteValues[1] = minQuoteValue;
        quoteValues[2] = maxQuoteValue;
        this.seller = seller;
    }

    /**
     * Get the value, min, and max for this {@link Quote}.
     *
     * @return the value, min, and max for this {@link Quote}.
     */
    public double[] getQuoteValues() {
        return quoteValues;
    }

    /**
     * Get the value for this {@link Quote}.
     *
     * @return the value for this {@link Quote}.
     */
    public double getQuoteValue() {
        return quoteValues[0];
    }

    /**
     * Get the total value for this {@link Quote} that includes the quote and moveCost.
     *
     * @return the value for this {@link Quote}.
     */
    public double getTotalQuote() {
        return getQuoteValue() + getMoveCost();
    }

    /**
     * Get the min for this {@link Quote}.
     *
     * @return the min for this {@link Quote}.
     */
    public double getQuoteMin() {
        return quoteValues[1];
    }

    /**
     * Get the max for this {@link Quote}.
     *
     * @return the max for this {@link Quote}.
     */
    public double getQuoteMax() {
        return quoteValues[2];
    }

    /**
     * Get the moveCost for this {@link Quote}.
     *
     * @return the moveCost for this {@link Quote}.
     */
    public double getMoveCost() {
        return moveCost;
    }

    /**
     * Set the value for this moveCost.
     *
     * @param moveCost The new value for this moveCost.
     */
    public void setMoveCost(double moveCost) {
        this.moveCost = moveCost;
    }

    /**
     * Whether this {@link Quote}s value is finite.
     *
     * @return Whether this {@link Quote}s value is finite.
     *         A finite value indicates the {@link ShoppingList} can be placed on the seller.
     */
    public boolean isFinite() {
        return Double.isFinite(quoteValues[0]);
    }

    /**
     * Whether this {@link Quote}s value is infinite.
     *
     * @return Whether this {@link Quote}s value is infinite.
     *         An infinite value indicates the {@link ShoppingList} cannot be placed on the seller.
     */
    public boolean isInfinite() {
        return Double.isInfinite(quoteValues[0]);
    }

    /**
     * Get the seller associated with this {@link Quote}.
     *
     * @return the seller associated with this {@link Quote}. Can be null.
     */
    @Nullable
    public Trader getSeller() {
        return seller;
    }

    public Optional<Context> getContext() {
        return Optional.empty();
    }

    @Nonnull
    public List<CommodityContext> getCommodityContexts() {
        return Collections.emptyList();
    }

    /**
     * Ranks are used to differentiate infinite quotes from each other.
     *
     * An infinite quote with a lower rank should be considered more informative than an infinite quote
     * with a higher rank. Infinite quotes with equal rank should be considered equally informative.
     *
     * All finite quotes should have a rank of zero. This number should never exceed {@link Quote#MAX_RANK}.
     *
     * @return The rank of the quote. Prefer quotes with lower ranks to quotes with higher ranks.
     */
    public abstract int getRank();

    /**
     * Get an explanation for the quote.
     *
     * Useful for explaining why an infinite quote was infinite.
     *
     * Finite quotes should return an empty explanation (empty String).
     *
     * @param shoppingList The shopping list associated with this quote.
     *
     * @return An explanation for the quote.
     */
    public abstract String getExplanation(@Nonnull final ShoppingList shoppingList);

    /**
     * A {@link MutableQuote} is a {@link Quote} that allows mutation of its
     * value, min, and max.
     */
    public static abstract class MutableQuote extends Quote {

        /**
         * Create a new mutable quote.
         * The value, min, and max will be initialized to 0.
         *
         * @param seller The seller associated with this {@link Quote}. Can be null.
         */
        protected MutableQuote(@Nullable final Trader seller) {
            super(seller);
        }

        /**
         * Create a new mutable quote with a given seller and value.
         * The min and max will be initialized to 0.
         *
         * @param seller The seller associated with this {@link Quote}. Can be null.
         * @param quoteValue The value for this quote.
         */
        protected MutableQuote(@Nullable final Trader seller, final double quoteValue) {
            super(seller, quoteValue, 0, 0);
        }

        /**
         * Create a new mutable quote with a given seller, value, min, and max.
         *
         * @param seller The seller associated with this {@link Quote}. Can be null.
         * @param quoteValue The value for this quote.
         * @param minQuoteValue The min for this quote.
         * @param maxQuoteValue The max for this quote.
         */
        protected MutableQuote(@Nullable final Trader seller, final double quoteValue,
                              final double minQuoteValue, final double maxQuoteValue) {
            super(seller, quoteValue, minQuoteValue, maxQuoteValue);
        }

        /**
         * Set the value for this quote.
         *
         * @param quoteValue The new value for this quote.
         */
        public void setQuoteValue(final double quoteValue) {
            quoteValues[0] = quoteValue;
        }

        /**
         * Set the value, min, and max for this quote.
         *
         * @param quoteValue The new value for this quote.
         * @param minQuoteValue The new min for this quote.
         * @param maxQuoteValue The new max for this quote.
         */
        public void setQuoteValues(final double quoteValue, final double minQuoteValue,
                                   final double maxQuoteValue) {
            quoteValues[0] = quoteValue;
            quoteValues[1] = minQuoteValue;
            quoteValues[2] = maxQuoteValue;
        }

        /**
         * Set the values for this quote.
         *
         * @param quoteValues The new values for this quote. Should be an array of 3 values.
         *                    At index 0, the value.
         *                    At index 1, the min,
         *                    At index 2, the max.
         */
        public void setQuoteValues(final double[] quoteValues) {
            this.quoteValues[0] = quoteValues[0];
            this.quoteValues[1] = quoteValues[1];
            this.quoteValues[2] = quoteValues[2];
        }

        /**
         * Add a cost to the min of the quote.
         *
         * @param additiveCost The cost to add.
         * @return The new min of the quote after adding the additiveCost.
         */
        public double addCostToMinQuote(final double additiveCost) {
            quoteValues[1] += additiveCost;
            return quoteValues[1];
        }

        /**
         * Add a cost to the max of the quote.
         *
         * @param additiveCost The cost to add.
         * @return The new max of the quote after adding the additiveCost.
         */
        public double addCostToMaxQuote(final double additiveCost) {
            quoteValues[2] += additiveCost;
            return quoteValues[2];
        }
    }

    /**
     * Same as CommodityQuote, but with additional information like the RegionId.
     */
    public static class CommodityCloudQuote extends CommodityQuote {

        // Context with extra data about the Quote
        protected Context moveContext;

        // Context with commodity data about the Quote
        protected List<CommodityContext> commodityContexts;

        protected CommodityCloudQuote(@Nullable final Trader seller,
                                      final double quoteValue,
                                      @Nullable final Long regionId,
                                      @Nullable final Long balanceAccountId,
                                      @Nullable final List<CommodityContext> commodityContexts) {
            super(seller, quoteValue);
            Context.Builder builder = Context.newBuilder();
            if (regionId != null) {
                builder.setRegionId(regionId);
            }
            if (balanceAccountId != null) {
                BalanceAccountDTO balanceAccount = BalanceAccountDTO.newBuilder().setId(balanceAccountId).build();
                builder.setBalanceAccount(balanceAccount);
            }
            moveContext = builder.build();
            this.commodityContexts = commodityContexts;
        }

        protected CommodityCloudQuote(@Nullable final Trader seller,
                                      final double quoteValue,
                                      @Nullable final Context context,
                                      @Nullable final Double requestedCoupons,
                                      @Nullable final Double allocatedCoupons,
                                      @Nonnull final UnmodifiableEconomy economy) {
            super(seller, quoteValue);
            if (context != null) {
                Context.Builder builder = Context.newBuilder();
                builder.setRegionId(context.getRegionId());
                builder.setZoneId(context.getZoneId());
                BalanceAccountDTO balanceAccount = BalanceAccountDTO.newBuilder()
                        .setId(context.getBalanceAccount().getId()).build();
                builder.setBalanceAccount(balanceAccount);
                builder.addFamilyBasedCoverage(CoverageEntry.newBuilder()
                    .setProviderId(seller.getOid())
                    .setTotalRequestedCoupons(requestedCoupons)
                    .setTotalAllocatedCoupons(allocatedCoupons));
                moveContext = builder.build();
            }
        }

        @Override
        public Optional<Context> getContext() {
            return Optional.ofNullable(moveContext);
        }

        @Override
        public @Nonnull List<CommodityContext> getCommodityContexts() {
            return commodityContexts == null ? Collections.emptyList() : commodityContexts;
        }
    }

    /**
     * An {@link com.vmturbo.platform.analysis.utilities.Quote.InitialInfiniteQuote} can be used
     * when initializing a quote. It is invalid with no seller and all values are infinite.
     */
    public static class InitialInfiniteQuote extends Quote {

        public InitialInfiniteQuote() {
            super(null, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
        }

        @Override
        public int getRank() {
            return INVALID_RANK;
        }

        @Override
        public String getExplanation(@Nonnull ShoppingList shoppingList) {
            return "no quote generated";
        }
    }

    /**
     * A quote whose value is computed relative to commodities being purchased.
     *
     * When the value of a {@link CommodityQuote} has an infinite value, it may
     * include information about which commodities had insufficient availability
     * which led to the infinite quote.
     */
    public static class CommodityQuote extends MutableQuote {
        protected List<InsufficientCommodity> insufficientCommodities = null;

        /**
         * Create new {@link CommodityQuote} for a given seller.
         * The value, min, and max of the quote are initialized to 0.
         *
         * @param seller The seller associated with this {@link CommodityQuote}.
         */
        public CommodityQuote(@Nullable final Trader seller) {
            super(seller);
        }

        /**
         * Create new {@link CommodityQuote} for a given seller with a given quote value.
         * The min and max of the quote are initialized to 0.
         *
         * @param seller The seller associated with this {@link CommodityQuote}.
         * @param quoteValue The value of the quote.
         */
        protected CommodityQuote(@Nullable final Trader seller, final double quoteValue) {
            super(seller, quoteValue, 0, 0);
        }

        /**
         * Create new {@link CommodityQuote} for a given seller with a given quote value, min, and max.
         *
         * @param seller The seller associated with this {@link CommodityQuote}.
         * @param quoteValue The value of the quote.
         * @param minQuoteValue The min of the quote.
         * @param maxQuoteValue The max of the quote.
         */
        public CommodityQuote(@Nullable final Trader seller, final double quoteValue,
                            final double minQuoteValue, final double maxQuoteValue) {
            super(seller, quoteValue, minQuoteValue, maxQuoteValue);
        }

        /**
         * {@inheritDoc}
         *
         * For a {@link CommodityQuote}, this is equal to the number of insufficient commodities
         * on the quote.
         *
         * @return The rank of the quote.
         */
        @Override
        public int getRank() {
            return getInsufficientCommodityCount();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getExplanation(@Nonnull final ShoppingList shoppingList) {
            if (getInsufficientCommodityCount() > 0) {
                return getInsufficientCommodities().stream()
                    .map(commodity -> insufficientCommodityString(commodity, shoppingList))
                    .collect(Collectors.joining(", and ")) +
                    (getSeller() == null ? "" :  " on seller " + getSeller());
            } else {
                return "Unexplained infinite quote";
            }
        }

        /**
         * Get the number of insufficient commodities associated with this quote.
         *
         * @return The number of insufficient commodities associated with this quote.
         */
        public int getInsufficientCommodityCount() {
            return insufficientCommodities == null ? 0 : insufficientCommodities.size();
        }

        /**
         * A static factory method to create a {@link CommodityQuote} an initial quote value of zero.
         *
         * @param seller The seller associated with this {@link Quote}.
         * @return A new {@link CommodityQuote} an initial quote value of zero.
         */
        public static CommodityQuote zero(@Nullable final Trader seller) {
            return new CommodityQuote(seller);
        }

        /**
         * A static factory method to create a {@link CommodityQuote} an initial quote value of infinite.
         *
         * @param seller The seller associated with this {@link Quote}.
         * @return A new {@link CommodityQuote} an initial quote value of infinite.
         */
        public static CommodityQuote infinite(@Nullable final Trader seller) {
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }

        /**
         * Get the insufficient commodities associated with this {@link CommodityQuote}.
         *
         * Returns an empty list if there are no insufficient commodities.
         *
         * @return the insufficient commodities associated with this {@link CommodityQuote}.
         */
        @Nonnull
        public List<InsufficientCommodity> getInsufficientCommodities() {
            return insufficientCommodities == null ? Collections.emptyList() : insufficientCommodities;
        }

        /**
         * Add a cost to the {@link CommodityQuote} for a particular commodity.
         *
         * @param additiveCost The additive cost to the quote contributed by the specific commodity.
         * @param sellerAvailableCapacity The available capacity of this commodity at the seller.
         * @param commodity The commodity whose cost is being contributed to the quote.
         * @return The value of the quote after contribution the additive cost for the commodity.
         */
        public double addCostToQuote(final double additiveCost,
                                     final double sellerAvailableCapacity,
                                     @Nonnull final CommoditySpecification commodity) {
            quoteValues[0] += additiveCost;
            if (Double.isInfinite(additiveCost)) {
                if (insufficientCommodities == null) {
                    insufficientCommodities = new ArrayList<>();
                }

                insufficientCommodities.add(new InsufficientCommodity(commodity, sellerAvailableCapacity));
            }

            return quoteValues[0];
        }

        /**
         * Add a cost to the {@link CommodityQuote} for a particular commodity.
         *
         * @param additiveCost The additive cost to the quote contributed by the specific commodity.
         * @param seller The seller contributing the quote.
         * @param soldIndex The index of the commodity whose cost is being added in the seller.
         * @return The value of the quote after contribution the additive cost for the commodity.
         */
        public double addCostToQuote(final double additiveCost,
                                     @Nonnull final Trader seller,
                                     final int soldIndex) {
            quoteValues[0] += additiveCost;
            if (Double.isInfinite(additiveCost)) {
                if (insufficientCommodities == null) {
                    insufficientCommodities = new ArrayList<>();
                }

                // The available capacity at the moment is the effective capacity minus the
                // quantity already in use.
                final CommoditySold commoditySold = seller.getCommoditiesSold().get(soldIndex);
                final double availableCapacity = commoditySold.getEffectiveCapacity() -
                    commoditySold.getQuantity();
                insufficientCommodities.add(new InsufficientCommodity(seller.getBasketSold().get(soldIndex),
                    availableCapacity));
            }

            return quoteValues[0];
        }

        /**
         * Get an explanation string for a particular insufficient commodity.
         *
         * @param insufficientCommodity The {@link InsufficientCommodity} whose values should be explained.
         * @param shoppingList The {@link ShoppingList} associated with this quote.
         * @return A string explaining the insufficient commodity with respect to the {@link ShoppingList}.
         */
        private String insufficientCommodityString(@Nonnull final InsufficientCommodity insufficientCommodity,
                                                   @Nonnull final ShoppingList shoppingList) {
            final CommoditySpecification commodity = insufficientCommodity.commodity;
            if (seller == null) {
                return "no available seller for commodity " + commodity.getDebugInfoNeverUseInCode();
            } else {
                return commodity.getDebugInfoNeverUseInCode() + " ("
                    + shoppingList.getQuantities()[shoppingList.getBasket().indexOf(commodity)] + "/"
                    + insufficientCommodity.availableQuantity + ")";
            }
        }
    }

    /**
     * A {@link LicenseUnavailableQuote} is generated when a license is unavailable.
     */
    public static class LicenseUnavailableQuote extends MutableQuote {
        private final CommoditySpecification licenseCommodity;

        /**
         * Create a new {@link LicenseUnavailableQuote} for infinite quotes when a license is
         * unavailable.
         *
         * @param seller The seller associated with the {@link Quote}.
         * @param licenseCommodity The license commodity.
         */
        public LicenseUnavailableQuote(@Nullable Trader seller,
                                       @Nonnull final CommoditySpecification licenseCommodity) {
            super(seller, Double.POSITIVE_INFINITY);

            this.licenseCommodity = licenseCommodity;
        }

        @Override
        public int getRank() {
            return MAX_RANK;
        }

        @Override
        public String getExplanation(@Nonnull final ShoppingList shoppingList) {
            return "License " + licenseCommodity.getDebugInfoNeverUseInCode() + " unavailable";
        }
    }

    /**
     * A {@link InfiniteBelowMinAboveMaxCapacityLimitationQuote} is generated when min/max capacity limitation is not met.
     */
    public static class InfiniteBelowMinAboveMaxCapacityLimitationQuote extends MutableQuote {
        private final CommoditySpecification commoditySpecification;
        private final CapacityLimitation capacityLimitation;
        private final double commodityQuantity;

        /**
         * Create a new {@link InfiniteBelowMinAboveMaxCapacityLimitationQuote}.
         *
         * @param commoditySpecification commodity.
         * @param capacityLimitation capacityLimitation for the commodity.
         * @param commodityQuantity commodity quantity.
         */
        public InfiniteBelowMinAboveMaxCapacityLimitationQuote(@Nonnull final CommoditySpecification commoditySpecification,
                                                               @Nonnull final CapacityLimitation capacityLimitation,
                                                               final double commodityQuantity) {
            super(null, Double.POSITIVE_INFINITY);
            this.commoditySpecification = Objects.requireNonNull(commoditySpecification);
            this.capacityLimitation = Objects.requireNonNull(capacityLimitation);
            this.commodityQuantity = commodityQuantity;
        }

        @Override
        public int getRank() {
            // (1 - diff / quantity) * MAX_RANK, where diff is the distance of commodityQuantity from capacityLimitation.
            // Rank is related to how far commodityQuantity is from limitation. With larger diff, rank is lower.
            // above max
            if (commodityQuantity > capacityLimitation.getMaxCapacity()) {
                return (int)(1 - Math.abs(commodityQuantity - capacityLimitation.getMaxCapacity()) / commodityQuantity) * MAX_RANK;
            }
            // below min
            return (int)(1 - Math.abs(commodityQuantity - capacityLimitation.getMinCapacity()) / commodityQuantity) * MAX_RANK;
        }

        @Override
        public String getExplanation(@Nonnull final ShoppingList shoppingList) {
            return "Commodity " + commoditySpecification.getDebugInfoNeverUseInCode() + " with quantity "
                    + commodityQuantity + " can't meet capacity limitation, where min is " + capacityLimitation.getMinCapacity()
                    + ", and max is " + capacityLimitation.getMaxCapacity();
        }
    }

    /**
     * Create a new {@link InfiniteRatioBasedResourceDependencyQuote} which describes an infinite {@link Quote}
     * due to ratio based dependent resource requirement that cannot be met.
     */
    public static class InfiniteRatioBasedResourceDependencyQuote extends MutableQuote {
        private final RatioBasedResourceDependency dependentResourcePair;
        private final double baseCommodityQuantity;
        private final double dependentCommodityQuantity;

        /**
         * Create a new {@link InfiniteRatioBasedResourceDependencyQuote}.
         *
         * @param dependentResourcePair The {@link RatioBasedResourceDependency} whose requirement cannot be met.
         * @param baseCommodityQuantity The quantity of the base commodity in the dependent pair.
         * @param dependentCommodityQuantity The quantity of the dependent commodity in the pair.
         */
        public InfiniteRatioBasedResourceDependencyQuote(@Nonnull final RatioBasedResourceDependency dependentResourcePair,
                                                         final double baseCommodityQuantity,
                                                         final double dependentCommodityQuantity) {
            super(null, Double.POSITIVE_INFINITY);

            this.dependentResourcePair = Objects.requireNonNull(dependentResourcePair);
            this.baseCommodityQuantity = baseCommodityQuantity;
            this.dependentCommodityQuantity = dependentCommodityQuantity;
        }

        @Override
        public int getRank() {
            // (1 - diff / quantity) * MAX_RANK, where diff is the distance of dependentCommodityQuantity
            // from the restricted value by constraint. With larger diff, rank is lower.
            final double restrictedDependentQuantity = dependentResourcePair.getMaxRatio() * baseCommodityQuantity;
            return (int)(1 - Math.abs(dependentCommodityQuantity
                    - restrictedDependentQuantity) / dependentCommodityQuantity) * MAX_RANK;
        }

        @Override
        public String getExplanation(@Nonnull final ShoppingList shoppingList) {
            return "Dependent commodity "
                + dependentResourcePair.getDependentCommodity().getDebugInfoNeverUseInCode() + " ("
                + dependentCommodityQuantity + ") exceeds base commodity "
                + dependentResourcePair.getBaseCommodity().getDebugInfoNeverUseInCode() + " ("
                + baseCommodityQuantity + ") [maxRatio=" + dependentResourcePair.getMaxRatio() + "]";
        }
    }

    /**
     * Create a new {@link InfiniteRangeBasedResourceDependencyQuote} which describes an infinite {@link Quote}
     * due to a range based dependent resource requirement that cannot be met.
     */
    public static class InfiniteRangeBasedResourceDependencyQuote extends MutableQuote {
        private final CommoditySpecification baseCommodityType;
        private final CommoditySpecification dependentCommodityType;
        private final Double dependentCommodityCapacity;
        private final double baseCommodityQuantity;
        private final double dependentCommodityQuantity;

        /**
         * Create a new {@link InfiniteRangeBasedResourceDependencyQuote}.
         *
         * @param baseCommodityType base commodity type.
         * @param dependentCommodityType dependent commodity type.
         * @param dependentCommodityCapacity The capacity of the dependent commodity in the dependent pair.
         * @param baseCommodityQuantity The quantity of the base commodity in the pair.
         * @param dependentCommodityQuantity The quantity of the dependent commodity in the pair.
         */
        public InfiniteRangeBasedResourceDependencyQuote(@Nonnull final CommoditySpecification baseCommodityType,
                                                            @Nonnull final CommoditySpecification dependentCommodityType,
                                                            @Nullable Double dependentCommodityCapacity,
                                                            @Nullable Double baseCommodityQuantity,
                                                            @Nullable Double dependentCommodityQuantity) {
            super(null, Double.POSITIVE_INFINITY);

            this.baseCommodityType = Objects.requireNonNull(baseCommodityType);
            this.dependentCommodityType = Objects.requireNonNull(dependentCommodityType);
            this.dependentCommodityCapacity = dependentCommodityCapacity;
            this.baseCommodityQuantity = baseCommodityQuantity;
            this.dependentCommodityQuantity = dependentCommodityQuantity;
        }

        @Override
        public int getRank() {
            // (1 - diff / quantity) * MAX_RANK, where diff is the distance of dependentCommodityQuantity
            // from the restricted value by constraint. With larger diff, rank is lower.
            return (int)(1 - Math.abs(dependentCommodityQuantity
                    - dependentCommodityCapacity) / dependentCommodityQuantity) * MAX_RANK;
        }

        @Override
        public String getExplanation(@Nonnull final ShoppingList shoppingList) {
            return "Dependent commodity " + dependentCommodityType.getDebugInfoNeverUseInCode() + " ("
                    + dependentCommodityQuantity + ") exceeds ranged capacity (" + dependentCommodityCapacity
                    + "), with base commodity " + baseCommodityType.getDebugInfoNeverUseInCode()
                    + " (" + baseCommodityQuantity + ").";
        }
    }

    /**
     * A new {@link InfiniteDependentComputeCommodityQuote} which describes an infinite {@link Quote}
     * due to a dependent resource requirement that cannot be met.
     */
    public static class InfiniteDependentComputeCommodityQuote extends MutableQuote {
        private final int comm1BoughtIndex;
        private final int comm2BoughtIndex;

        private final double comm1SoldCapacity;
        private final double comm1BoughtQuantity;
        private final double comm2BoughtQuantity;

        /**
         * Create a new {@link InfiniteDependentComputeCommodityQuote} which describes an infinite {@link Quote}
         * due to a dependent resource requirement that cannot be met.
         *
         * @param comm1BoughtIndex The index of the first commodity bought in the dependent pair.
         * @param comm2BoughtIndex The index of the second commodity bought in the dependent pair.
         * @param comm1SoldCapacity The sold capacity of the first commodity.
         * @param comm1BoughtQuantity The quantity of the first commodity bought.
         * @param comm2BoughtQuantity The quantity of the second commodity bought.
         */
        public InfiniteDependentComputeCommodityQuote(final int comm1BoughtIndex,
                                                      final int comm2BoughtIndex,
                                                      final double comm1SoldCapacity,
                                                      final double comm1BoughtQuantity,
                                                      final double comm2BoughtQuantity) {
            super(null, Double.POSITIVE_INFINITY);

            this.comm1BoughtIndex = comm1BoughtIndex;
            this.comm2BoughtIndex = comm2BoughtIndex;
            this.comm1SoldCapacity = comm1SoldCapacity;
            this.comm1BoughtQuantity = comm1BoughtQuantity;
            this.comm2BoughtQuantity = comm2BoughtQuantity;
        }

        @Override
        public int getRank() {
            return MAX_RANK;
        }

        @Override
        public String getExplanation(@Nonnull final ShoppingList shoppingList) {
            return "Dependent compute commodities " + specNameFor(shoppingList, comm1BoughtIndex) + " and "
                + specNameFor(shoppingList, comm2BoughtIndex) + " sum (" + comm1BoughtQuantity + " + "
                + comm2BoughtQuantity + " = " + (comm1BoughtQuantity + comm2BoughtQuantity)
                + ") exceeds capacity " + comm1SoldCapacity;
        }

        /**
         * Get the debug info of the commodity specification for a given index.
         *
         * @param shoppingList The shopping list whose basket the specification should be retrieved from.
         * @param commoditySpecificationIndex The index of the specification to retrieve.
         * @return The {@link CommoditySpecification} at the given index. If the index is less than
         *         zero, returns null.
         */
        @Nullable
        private String specNameFor(@Nonnull final ShoppingList shoppingList,
                                                   final int commoditySpecificationIndex) {
            return commoditySpecificationIndex >= 0 ?
                shoppingList.getBasket().get(commoditySpecificationIndex)
                    .getDebugInfoNeverUseInCode() :
                null;
        }
    }

    /**
     * An {@link InsufficientCommodity} expresses, for a commodity, that a quantity of that
     * commodity was requested that was greater than the supply available.
     *
     * Note that the requested amount is not stored on the object itself and can be pulled
     * from the {@link ShoppingList} that this {@link Quote} is associated with.
     *
     * Note that the amount available on a supplier may change as shopping lists are
     * moved around between different traders. The availableQuantity tracked here is
     * fixed to the amount that was available when the {@link Quote} was generated.
     */
    @Immutable
    public static class InsufficientCommodity {
        /**
         * The index of the commodity in the shopping list basket.
         */
        public final CommoditySpecification commodity;

        /**
         * The quantity of the commodity available at the seller.
         */
        public final double availableQuantity;

        /**
         * Create a new {@link InsufficientCommodity}.
         *
         * @param commodity The commodity whose supply was found insufficient on a particular supplier.
         * @param availableQuantity The amount available at that supplier.
         */
        public InsufficientCommodity(@Nonnull final CommoditySpecification commodity,
                                     final double availableQuantity) {
            this.commodity = Objects.requireNonNull(commodity);
            this.availableQuantity = availableQuantity;
        }
    }

    /**
     * class to represent commodity context on a specific seller.
     */
    public static class CommodityContext {

        private final CommoditySpecification commodity;
        private final double newCapacity;
        /**
         * A commodity is considered to be decisive on the seller if buyer's commodity capacity is configured
         * by user instead of some constant values. For example, GP2 StorageTier as a seller, storageAmount
         * is decisive because when user creates a GP2 volume, they need to configure the volume size(i.e.
         * storageAmount capacity for the volume).
         *
         * <p>The field is used to check whether there is commodity resize when QuoteMinimizer thinks the best
         * seller for the buyer is its current supplier. After analysis, when best seller is current supplier,
         * decisive commodities' new capacities are compared with buyer's old capacities to decide whether
         * the decisive commodities should be resized.
         */
        private boolean isDecisive;

        /**
         * Initialize a CommodityContext which represents commodity capacity on specific seller.
         *
         * @param commodity commodity specification.
         * @param newCapacity new capacity on the seller.
         * @param isDecisive whether commodity is decisive on the seller.
         */
        public CommodityContext(@Nonnull CommoditySpecification commodity,
                                double newCapacity, boolean isDecisive) {
            this.commodity = commodity;
            this.newCapacity = newCapacity;
            this.isDecisive = isDecisive;
        }

        /**
         * Commodity type.
         *
         * @return commodity specification for the CommodityContext.
         */
        public CommoditySpecification getCommoditySpecification() {
            return commodity;
        }

        /**
         * Commodity new capacity at the seller.
         *
         * @return new capacity for the commodity at the seller.
         */
        public double getNewCapacityOnSeller() {
            return newCapacity;
        }

        /**
         * Whether commodity is decisive on the seller.
         *
         * @return true if commodity is decisive on the seller.
         */
        public boolean isCommodityDecisiveOnSeller() {
            return isDecisive;
        }
    }
}

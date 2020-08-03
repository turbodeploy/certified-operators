package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.BuyRIExplanation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceDerivedCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.RIBuyRateProvider.PricingProviderResult;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyRegionalContext;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This class represents a single recommended action to be taken for reserved instances, eg
 * "buy 3 shared-tenancy Linux m4.medium standard reserved instances
 * in us-east-1, with a 1 year term, paying all upfront", along with associated cost information.
 *
 * The output of the recommendation algorithm will include 0 or more instances of this class.
 */
public class ReservedInstanceAnalysisRecommendation {

    private static final Logger logger = LogManager.getLogger();

    // The Id of the buy RI record in the buy_reserved_instance table.
    private long buyRiId;

    // What is the context in which this reserved instance was bought; e.g. instanceType, region, platform, tenancy
    private final RIBuyRegionalContext regionalContext;

    private final long purchasingAccountOid;

    // How many reservations to do the action with, eg BUY 17 of them.
    private final int count;

    // The on-demand hourly cost without any RI discount
    private final float onDemandHourlyCost;

    // The total amortized RI hourly cost (actual hourly + amortized upfront) in dollars of each
    // reserved instance. This is per-instance, that is to say, you must multiply by the count
    // to get the total cost of the reservation. Example: if an RI with a 1 year term has a partial
    // up front payment of $262800 plus an hourly charge of $40, this field would be $70
    // ($262800 / 1 / 365 / 24) + $40 == $30 + $40 == $70 amortized hourly cost.
    private final float riHourlyCost;

    /**
     * The hourly upfront rate for a recommended RI. The value will be 0 when no upfront purchase
     * profile is selected.
     */
    private final float riUpFrontRate;

    /**
     * The hourly recurring rate for a recommended RI. The value will be 0 when all upfront purchase
     * profile is selected.
     */
    private final float riRecurringRate;

    /**
     * The hourly savings, in dollars, over the on-demand cost as a function of the RI's utilization
     * rate. This is overall savings for all the RI recommended to buy.
     * Example: If the On-demand cost is $100, the RI cost is $70, 2RIs recommended to but then at
     * 100% RI utilization this value would be $60, and less as the utilization drops.
     *
     * This value should not be divided on RI count to buy, as savings are not proportional to
     * RI count.
     */
    private final float hourlyCostSavings;

    // Average coupon demand.  Sum up the demand across all the hours / hours.  In coupons.
    private final float averageCouponDemand;

    // number of Hours, how many data points used?
    private final int numberOfHours;

    // For count number of RIs, X, the number of coupons that X can cover
    // this is instance count.
    private final int riNormalizedCoupons;

    // For count number of RIs, X, the actual number of coupons of demand that X covers.
    private final float riNormalizedCouponsUsed;

    // A tag to find related log entries, which will have the tag in their log output.
    private final String logTag;

    // For Buy action, coverage goal, either maximal or percentage.
    private final String actionGoal;

    // How many active hours were used in the calculations, hours where usage was not zero.
    private final int activeHours;

    // Not final because we will set it only when calling createBuyRiInfo
    private ReservedInstanceBoughtInfo riBoughtInfo;

    private final ReservedInstanceSpec riSpec;

    /**
     * The dollar amount you would pay for the workloads if you don't buy this RI.
     */
    private float estimatedOnDemandCost;

    /*
     How much hourly demand each template had for a week based on which this RI Buy recommendation
     was generated.
    */
    private Map<TopologyEntityDTO, float[]> templateTypeHourlyDemand;

    public ReservedInstanceAnalysisRecommendation(@Nonnull String logTag,
                                                  @Nonnull String actionGoal,
                                                  @Nonnull RIBuyRegionalContext regionalContext,
                                                  long purchasingAccountOid,
                                                  int count,
                                                  @Nonnull PricingProviderResult pricing,
                                                  float hourlyCostSavings,
                                                  float averageCouponDemand,
                                                  int numberOfHours,
                                                  int activeHours,
                                                  int riNormalizedCouponsCoupons,
                                                  float riNormalizedCouponsUsed) {
        this.logTag = Objects.requireNonNull(logTag);
        this.actionGoal = Objects.requireNonNull(actionGoal);
        this.regionalContext = Objects.requireNonNull(regionalContext);
        this.purchasingAccountOid = purchasingAccountOid;
        this.count = count;
        this.onDemandHourlyCost = pricing.onDemandRate();
        this.riHourlyCost = pricing.reservedInstanceRate();
        this.riUpFrontRate = pricing.reservedInstanceUpfrontRate();
        this.riRecurringRate = pricing.reservedInstanceRecurringRate();
        this.hourlyCostSavings = hourlyCostSavings;
        this.averageCouponDemand = averageCouponDemand;
        this.numberOfHours = numberOfHours;
        this.activeHours = activeHours;
        this.riNormalizedCoupons = riNormalizedCouponsCoupons;
        this.riNormalizedCouponsUsed = riNormalizedCouponsUsed;
        this.riSpec = Objects.requireNonNull(regionalContext.riSpecToPurchase());
        this.riBoughtInfo = createRiBoughtInfo();
    }

    /**
     * Get the header string to use in a CSV file of recommendations.
     *
     * @return a CSV header string, without a trailing newline.
     */
    public static String getCSVHeader() {
        return csvHeader;
    }


    private static final String csvHeader = ",Log Key,Buy Type,Master Account,Instance Type,"
                                            + "Location,Location OID,Platform,Tenancy,Offering Class,Term,Payment Option,"
                    + "Count,hourly onDemand cost,hourly RI cost,hourly savings,"
                    + "discount cost,discount %,savings %,# hours,active hours,avg coupon demand,"
                    + "RI coupons,RI coupons used,RI utilization";

    /**
     * Create a CSV describing a recommendation.
     *
     * @return a string with a CSV representation of a recommendation.
     */
    public String toCSVString() {
        // None of the fields currently can contain a comma.
        // If this changes, consider switching to using Apache Commons CSV.

        // is riHourlyCost known, unknown is sent as Float.MAX_VALUE
        boolean isRiHourlyCostKnown = riHourlyCost != Float.MAX_VALUE;

        // Make informative String for RiHourlyCost
        String riHourlyCostString = isRiHourlyCostKnown
            ? String.format(Locale.ROOT, "%.5f", riHourlyCost)
            : "unknown";
        // Make informative string for discount percentage when RIHourlyCost is not known
        String discountPercentageString = isRiHourlyCostKnown
            ? String.format(Locale.ROOT, "%.5f%%",
                            ((onDemandHourlyCost - riHourlyCost) / onDemandHourlyCost) * 100.0f)
            : "unknown";

        StringJoiner joiner = new StringJoiner(",");

        final ReservedInstanceSpecInfo riSpecInfo = riSpec.getReservedInstanceSpecInfo();
        final ReservedInstanceType riSpecType = riSpec.getReservedInstanceSpecInfo().getType();
        // initial comma allows adding log snippet into Excel as a csv, ignoring the first field,
        // which is inserted by the logger and delimited by this initial comma.
        joiner.add("," + logTag)
                .add(actionGoal)
                .add(Long.toString(purchasingAccountOid))
                .add(regionalContext.computeTier().getDisplayName())
                .add(regionalContext.region().getDisplayName())
                .add(Long.toString(regionalContext.regionOid()))
                .add(riSpecInfo.getOs().toString())
                .add(riSpecInfo.getTenancy().name())
                .add(riSpecType.getOfferingClass().toString())
                .add(Integer.toString(riSpecType.getTermYears()))
                .add(riSpecType.getPaymentOption().name())
                .add(Integer.toString(count))
                .add(String.format(Locale.ROOT, "%.5f", onDemandHourlyCost))
                .add(riHourlyCostString)
                .add(String.format(Locale.ROOT, "%.5f", hourlyCostSavings))
                .add(String.format(Locale.ROOT, "%.5f", onDemandHourlyCost - hourlyCostSavings))
                .add(discountPercentageString)
                .add(String.format(Locale.ROOT, "%.5f%%", (hourlyCostSavings/onDemandHourlyCost)*100.0f))
                .add(Integer.toString(numberOfHours))
                .add(Integer.toString(activeHours))
                .add(String.format(Locale.ROOT, "%.5f", averageCouponDemand))
                .add(Integer.toString(riNormalizedCoupons))
                .add(String.format(Locale.ROOT, "%.5f", riNormalizedCouponsUsed));
        float riUtilization = riNormalizedCoupons > 0.0f ? getRiUtilization()*100.0f : 0.0f;
        joiner.add(String.format(Locale.ROOT, "%.5f%%", riUtilization));

        return joiner.toString();
    }

    @Nonnull
    public TopologyEntityDTO getComputeTier() {
        return regionalContext.computeTier();
    }

    @Nonnull
    public long getRegion() {
        return regionalContext.regionOid();
    }

    @Nonnull
    public OSType getPlatform() {
        return riSpec.getReservedInstanceSpecInfo().getOs();
    }

    @Nonnull
    public Tenancy getTenancy() {
        return riSpec.getReservedInstanceSpecInfo().getTenancy();
    }

    public int getTermInYears() {
        return riSpec.getReservedInstanceSpecInfo().getType().getTermYears();
    }

    public int getCount() {
        return count;
    }

    public double getRiHourlyCost() {
        return riHourlyCost;
    }

    public double getOnDemandHourlyCost() {
        return onDemandHourlyCost;
    }

    public float getHourlyCostSavings() {
        return hourlyCostSavings;
    }

    public float getAverageHourlyCouponDemand() {
        return averageCouponDemand;
    }

    public int getRiNormalizedCoupons() {
        return riNormalizedCoupons;
    }

    public float getRiNormalizedCouponsUsed() {
        return riNormalizedCouponsUsed;
    }

    public float getRiUtilization() {
        return riNormalizedCouponsUsed / riNormalizedCoupons;
    }

    public float getVmCoverage() {
        return riNormalizedCouponsUsed / averageCouponDemand;
    }

    public String getLogTag() {
        return logTag;
    }

    public String getActionGoal() {
        return actionGoal;
    }

    public int getActiveHours() {
        return activeHours;
    }

    public ReservedInstanceSpec getRiSpec() {
        return riSpec;
    }

    @Nonnull
    public ReservedInstanceBoughtInfo getRiBoughtInfo() {
        return riBoughtInfo;
    }

    public long getBuyRiId() {
        return buyRiId;
    }

    public Map<TopologyEntityDTO, float[]> getTemplateTypeHourlyDemand() {
        return templateTypeHourlyDemand;
    }

    public void setTemplateTypeHourlyDemand(final Map<TopologyEntityDTO, float[]> templateTypeHourlyDemand) {
        this.templateTypeHourlyDemand = templateTypeHourlyDemand;
    }

    /**
     * Gets the dollar amount you would pay for the workloads if you don't buy this RI.
     *
     * @return on demand dollar amount.
     */
    public float getEstimatedOnDemandCost() {
        return estimatedOnDemandCost;
    }

    /**
     * Gets the dollar amount you would pay for the workloads if you don't buy this RI.
     *
     * @param estimatedOnDemandCost The estimated OnDemand Cost.
     */
    public void setEstimatedOnDemandCost(final float estimatedOnDemandCost) {
        this.estimatedOnDemandCost = estimatedOnDemandCost;
    }

    /**
     * Creates the RI Bought object from the buy RI recommendation.
     *
     * @return The reserved instance bought info representing this Buy RI recommendation.
     */
    public ReservedInstanceBoughtInfo createRiBoughtInfo() {

        ReservedInstanceBoughtCost.Builder riBoughtCost = ReservedInstanceBoughtCost.newBuilder()
            .setFixedCost(CurrencyAmount.newBuilder().setAmount(riUpFrontRate *
                    RIBuyRateProvider.HOURS_IN_A_MONTH * 12 * getTermInYears()).build())
            .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(riRecurringRate).build())
            .setUsageCostPerHour(CurrencyAmount.newBuilder().setAmount(riHourlyCost).build());

        final ReservedInstanceDerivedCost riDerivedCost = ReservedInstanceDerivedCost.newBuilder()
                .setAmortizedCostPerHour(
                        CurrencyAmount.newBuilder()
                                .setAmount(riHourlyCost)
                                .build())
                .setOnDemandRatePerHour(
                        CurrencyAmount.newBuilder()
                                .setAmount(onDemandHourlyCost)
                                .build())
                .build();

        final int numOfCoupons = regionalContext.couponsPerRecommendedInstance();

        ReservedInstanceBoughtCoupons.Builder riBoughtCoupons = ReservedInstanceBoughtCoupons.newBuilder()
            .setNumberOfCoupons(riNormalizedCoupons * numOfCoupons);
        riBoughtCoupons.setNumberOfCouponsUsed(riNormalizedCouponsUsed * numOfCoupons);

        ReservedInstanceBoughtInfo.Builder riBoughtInfoBuilder = ReservedInstanceBoughtInfo.newBuilder()
                .setBusinessAccountId(purchasingAccountOid)
                .setNumBought(getCount())
                .setReservedInstanceSpec(getRiSpec().getId())
                .setReservedInstanceBoughtCost(riBoughtCost)
                .setReservedInstanceDerivedCost(riDerivedCost)
                .setReservedInstanceBoughtCoupons(riBoughtCoupons)
                .setStartTime(Instant.now().toEpochMilli());
        return riBoughtInfoBuilder.build();
    }

    /**
     *  Create Action object from the RI Recommendation.
     *
     * @return Action DTO
     */
    public ActionDTO.Action createAction() {
        BuyRI buyRI = BuyRI.newBuilder()
            .setBuyRiId(buyRiId)
            .setComputeTier(ActionEntity.newBuilder()
                .setId(getComputeTier().getOid())
                .setType(getComputeTier().getEntityType())
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD))
            .setCount(getCount())
            .setRegion(ActionEntity.newBuilder().setId(getRegion())
                    .setType(EntityType.REGION_VALUE)
                    .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                    .build())
            .setMasterAccount(ActionEntity.newBuilder().setId(purchasingAccountOid)
                    .setType(EntityType.BUSINESS_ACCOUNT_VALUE)
                    .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                    .build())
            .build();

        final int instanceTypeCoupons = regionalContext.couponsPerRecommendedInstance();
        final float totalAverageDemand = averageCouponDemand * instanceTypeCoupons;
        float coveredAverageDemand = getRiUtilization() * count * instanceTypeCoupons;
        float estimatedOnDemandCost = getEstimatedOnDemandCost();

        Explanation explanation = Explanation.newBuilder()
                .setBuyRI(BuyRIExplanation.newBuilder()
                        .setCoveredAverageDemand(coveredAverageDemand)
                        .setTotalAverageDemand(totalAverageDemand)
                        .setEstimatedOnDemandCost(estimatedOnDemandCost)
                        .build())
                .build();

        ActionDTO.Action action =
                ActionDTO.Action.newBuilder()
                        .setId(IdentityGenerator.next())
                        .setInfo(ActionInfo.newBuilder()
                                .setBuyRi(buyRI)
                                .build())
                        .setExplanation(explanation)
                        .setDeprecatedImportance(0)
                        .setSupportingLevel(SupportLevel.SHOW_ONLY)
                        .setSavingsPerHour(CurrencyAmount.newBuilder()
                                .setAmount(hourlyCostSavings * count).build())
                        .setExecutable(false)
                        .build();

        return action;
    }

    /**
     * Set the Id of the buy RI record in the buy_reserved_instance table.
     *
     * @param buyRiId
     */
    public void setBuyRiId(final long buyRiId) {
        this.buyRiId = buyRiId;
    }
}

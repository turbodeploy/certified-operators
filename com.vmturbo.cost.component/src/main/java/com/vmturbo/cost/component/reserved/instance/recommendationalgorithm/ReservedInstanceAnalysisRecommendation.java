package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Locale;
import java.util.Objects;
import java.util.StringJoiner;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.setting.RISettingsEnum.PreferredOfferingClass;
import com.vmturbo.components.common.setting.RISettingsEnum.PreferredPaymentOption;
import com.vmturbo.cost.component.reserved.instance.BuyReservedInstanceStore.BuyReservedInstanceInfo;
import com.vmturbo.cost.component.reserved.instance.ImmutableBuyReservedInstanceInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
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

    /**
     * Possible actions that might be taken with respect to a reserved instance,
     * eg BUY, SELL, MODIFY, CONVERT, EXCHANGE. Currently only BUY.
     */
    public enum Action {
        BUY("Buy");

        private String label;

        Action(String label) {
            this.label = label;
        }

        public String getLabel() { return label; }
    };

    // What is the context in which this reserved instance was bought; e.g. instanceType, region, platform, tenancy
    private final ReservedInstanceRegionalContext context;

    // What are the purchasing constraints; e.g. offeringClass, years, payment option
    private final ReservedInstancePurchaseConstraints constraints;

    // Recommended action with respect to the reservation, eg BUY some, SELL some, etc. Currently only BUY.
    private final Action action;

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

    public ReservedInstanceAnalysisRecommendation(@Nonnull String logTag,
                                                  @Nonnull String actionGoal,
                                                  @Nonnull ReservedInstanceRegionalContext context,
                                                  @Nonnull ReservedInstancePurchaseConstraints contraints,
                                                  @Nonnull Action action,
                                                  int count,
                                                  float onDemandHourlyCost,
                                                  float riHourlyCost,
                                                  float hourlyCostSavings,
                                                  float averageCouponDemand,
                                                  int numberOfHours,
                                                  int activeHours,
                                                  int riNormalizedCouponsCoupons,
                                                  float riNormalizedCouponsUsed,
                                                  @Nonnull ReservedInstanceSpec riSpec) {
        this.logTag = Objects.requireNonNull(logTag);
        this.actionGoal = Objects.requireNonNull(actionGoal);
        this.context = Objects.requireNonNull(context);
        this.constraints = Objects.requireNonNull(contraints);
        this.action = Objects.requireNonNull(action);
        this.count = count;
        this.onDemandHourlyCost = onDemandHourlyCost;
        this.riHourlyCost = riHourlyCost;
        this.hourlyCostSavings = hourlyCostSavings;
        this.averageCouponDemand = averageCouponDemand;
        this.numberOfHours = numberOfHours;
        this.activeHours = activeHours;
        this.riNormalizedCoupons = riNormalizedCouponsCoupons;
        this.riNormalizedCouponsUsed = riNormalizedCouponsUsed;
        this.riSpec = Objects.requireNonNull(riSpec);
    }

    /**
     * Get the header string to use in a CSV file of recommendations.
     *
     * @return a CSV header string, without a trailing newline.
     */
    public static String getCSVHeader() {
        return csvHeader;
    }


    private static final String csvHeader = "Log Key,Buy Type,Master Account,"
                    + "Instance Type,Location,Platform,Tenancy,Offering Class,Term,Payment Option,"
                    + "Action,Count,hourly onDemand cost,hourly RI cost,hourly savings,"
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

        // From context get master account and linked account, if any
        long masterAccountId = context.getMasterAccount();

        StringJoiner joiner = new StringJoiner(",");

        joiner.add(logTag)
                .add(actionGoal)
                .add(Long.toString(context.getMasterAccount()))
                .add(context.getComputeTier().getDisplayName())
                .add(Long.toString(context.getRegion()))
                .add(context.getPlatform().toString())
                .add(context.getTenancy().name())
                .add(constraints.getOfferingClass().toString())
                .add(Integer.toString(constraints.getTermInYears()))
                .add(constraints.getPaymentOption().name())
                .add(action.toString())
                .add(Integer.toString(count))
                .add(String.format(Locale.ROOT, "%.5f", onDemandHourlyCost))
                .add(String.format(Locale.ROOT, "%.5f", riHourlyCost))
                .add(String.format(Locale.ROOT, "%.5f", hourlyCostSavings))
                .add(String.format(Locale.ROOT, "%.5f", onDemandHourlyCost - hourlyCostSavings))
                .add(String.format(Locale.ROOT, "%.5f%%", ((onDemandHourlyCost - riHourlyCost)/onDemandHourlyCost)*100.0f))
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
        return context.getComputeTier();
    }

    @Nonnull
    public long getRegion() {
        return context.getRegion();
    }

    @Nonnull
    public OSType getPlatform() {
        return context.getPlatform();
    }

    @Nonnull
    public Tenancy getTenancy() {
        return context.getTenancy();
    }

    @Nonnull
    public PreferredOfferingClass getOfferingClass() {
        return constraints.getOfferingClass();
    }

    public int getTermInYears() {
        return constraints.getTermInYears();
    }

    @Nonnull
    public PreferredPaymentOption getPaymentOption() {
        return constraints.getPaymentOption();
    }

    @Nonnull
    public Action getAction() {
        return action;
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

    /**
     *  Create Action object from the RI Recommendation.
     *
     * @return Action DTO
     */
    public ActionDTO.Action createAction() {

        String explanationString = getAction().getLabel() + " " + getCount() + " '"
                + getComputeTier().getDisplayName() + "' RIs for "
                + getRegion();

        logger.info(explanationString);
        ActionEntity entityToClone = ActionEntity.newBuilder()
            .setId(getComputeTier().getOid())
            .setType(EntityType.COMPUTE_TIER_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

        Provision provision = Provision.newBuilder()
                .setEntityToClone(entityToClone)
                .setProvisionedSeller(getComputeTier().getOid())
                .build();

        Explanation explanation = Explanation.newBuilder()
                .setProvision(ProvisionExplanation.newBuilder().build())
                .build();

        ActionDTO.Action action =
                ActionDTO.Action.newBuilder()
                        .setId(IdentityGenerator.next())
                        .setInfo(ActionInfo.newBuilder()
                                .setProvision(provision)
                                .build())
                        .setExplanation(explanation)
                        .setImportance(0)
                        .setSupportingLevel(SupportLevel.SHOW_ONLY)
                        .setExecutable(false)
                        .build();

        return action;
    }

    // Creates the RI Bought object from the buy RI recommendation
    public BuyReservedInstanceInfo createBuyRiInfo(long topologyContextId) {
        if (riBoughtInfo == null) {
            // TODO: Set the costs once it is ready
            ReservedInstanceBoughtCost.Builder riBoughtCost = ReservedInstanceBoughtCost.newBuilder()
                    .setFixedCost(CurrencyAmount.newBuilder().setAmount(0f).build())
                    .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(riHourlyCost).build())
                    .setUsageCostPerHour(CurrencyAmount.newBuilder().setAmount(0f).build());
            // TODO: The numberOfCoupons needs to be = (number of instances bought * number of
            // coupons per instance). Check if riNormalizedCoupons is returning that.
            ReservedInstanceBoughtCoupons.Builder riBoughtCoupons = ReservedInstanceBoughtCoupons.newBuilder()
                    .setNumberOfCoupons(riNormalizedCoupons);
            ReservedInstanceBoughtInfo.Builder riBoughtInfoBuilder = ReservedInstanceBoughtInfo.newBuilder()
                    .setBusinessAccountId(context.getMasterAccount())
                    .setNumBought(getCount())
                    .setReservedInstanceSpec(getRiSpec().getId())
                    .setReservedInstanceBoughtCost(riBoughtCost)
                    .setReservedInstanceBoughtCoupons(riBoughtCoupons);
            riBoughtInfo = riBoughtInfoBuilder.build();
        }
        return ImmutableBuyReservedInstanceInfo.builder()
                .riBoughtInfo(riBoughtInfo).riSpec(riSpec).topologyContextId(topologyContextId).build();
    }
}

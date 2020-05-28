package com.vmturbo.cost.component.reserved.instance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;

/**
 * Class that houses calculations related to Reserved Instance Costs.
 */
public class ReservedInstanceCostCalculator {

    private static final int NO_OF_MONTHS = 12;

    private static final int MONTHLY_TO_HOURLY_CONVERSION = 730;

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Constructor for ReservedInstanceSpecStore.
     *
     * @param reservedInstanceSpecStore object of type ReservedInstanceSpecStore.
     */
    public ReservedInstanceCostCalculator(@Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore) {
        this.reservedInstanceSpecStore = reservedInstanceSpecStore;
    }

    /**
     * Method that will calculate the per instance amortized cost for a given list of ReservedInstanceBoughtInfos.
     * This method looks up the term of an RI from the reserved instance spec store. This is then used
     * to calculate the per instance amortized cost = (fixedCost/730 * 12 * term) + recurringCost.
     *
     * @param reservedInstanceBoughtInfos List of ReservedInstanceBoughtInfo.
     * @param context a DSLContext object.
     * @return Map of getProbeReservedInstanceId -> amortizedCost.
     */
    public Map<String, Double> calculateReservedInstanceAmortizedCost(@Nonnull final List<ReservedInstanceBoughtInfo> reservedInstanceBoughtInfos,
                    DSLContext context) {
        @Nonnull final Map<Long, Integer> riSpecToTermMap =
            getRiSpecIdToTermInYearMap(reservedInstanceBoughtInfos, context);
        return calculateReservedInstanceAmortizedCost(reservedInstanceBoughtInfos, riSpecToTermMap);
    }

    /**
     * Method that will calculate the per instance amortized cost for a given list of ReservedInstanceBoughtInfos.
     * This method looks up the term of an RI from the reserved instance spec store. This is then used
     * to calculate the per instance amortized cost = (fixedCost/730 * 12 * term) + recurringCost.
     *
     * @param reservedInstanceBoughtInfos List of ReservedInstanceBoughtInfo.
     * @param riSpecToTermMap A map from RI spec Id used in the list of input RIs to their term
     *                        in years.
     * @return Map of getProbeReservedInstanceId -> amortizedCost.
     */
    public Map<String, Double> calculateReservedInstanceAmortizedCost(@Nonnull final List<ReservedInstanceBoughtInfo> reservedInstanceBoughtInfos,
                                                                      @Nonnull final Map<Long, Integer> riSpecToTermMap) {

        final Map<String, Double> probeRIIDToAmortizedCost = new HashMap<>();
        for (ReservedInstanceBoughtInfo reservedInstanceBoughtInfo : reservedInstanceBoughtInfos) {
            final ReservedInstanceBoughtCost reservedInstanceBoughtCost = reservedInstanceBoughtInfo.getReservedInstanceBoughtCost();
            final double fixedCost = reservedInstanceBoughtCost.getFixedCost().getAmount();
            final double recurringCost = reservedInstanceBoughtCost.getRecurringCostPerHour().getAmount();
            final long reservedInstanceSpec = reservedInstanceBoughtInfo.getReservedInstanceSpec();
            final String probeReservedInstanceId = reservedInstanceBoughtInfo.getProbeReservedInstanceId();
            final Integer riSpecTerm = riSpecToTermMap.get(reservedInstanceSpec);

            if (riSpecTerm == null) {
                logger.error("Unable to get Term information for RI with SpecID {} and probeReservedInstanceID {}. Amortized cost for RIs with this specID cannot be calculated and is set to 0.",
                                reservedInstanceSpec,
                                probeReservedInstanceId);
                probeRIIDToAmortizedCost.put(probeReservedInstanceId, 0D);
            } else if (riSpecTerm == 0) {
                logger.error("Term for RI Specification with riSpecID {} is 0. Unable to calculate amortized cost. Setting it to 0.",
                                reservedInstanceSpec);
                probeRIIDToAmortizedCost.put(probeReservedInstanceId, 0D);
            } else {
                final double amortizedCost = (fixedCost / (riSpecTerm * NO_OF_MONTHS * MONTHLY_TO_HOURLY_CONVERSION)) + recurringCost;
                probeRIIDToAmortizedCost.put(probeReservedInstanceId, amortizedCost);
            }
        }
        return probeRIIDToAmortizedCost;
    }

    /**
     * For input list of RI bought returns a map from associated spec IDs to their terms.
     *
     * @param reservedInstanceBoughtInfos the list of RI bought.
     * @param context a DSLContext object.
     * @return map from specs ids to their term in year.
     */
    public Map<Long, Integer> getRiSpecIdToTermInYearMap(@Nonnull final List<ReservedInstanceBoughtInfo> reservedInstanceBoughtInfos,
                    DSLContext context) {
        final Set<Long> riSpecIdSet = reservedInstanceBoughtInfos.stream()
            .map(ReservedInstanceBoughtInfo::getReservedInstanceSpec)
            .collect(Collectors.toSet());

        return reservedInstanceSpecStore.getReservedInstanceSpecByIdsWithContext(riSpecIdSet, context).stream()
            .collect(Collectors.toMap(Cost.ReservedInstanceSpec::getId,
                a -> a.getReservedInstanceSpecInfo()
                    .getType().getTermYears()));
    }
}

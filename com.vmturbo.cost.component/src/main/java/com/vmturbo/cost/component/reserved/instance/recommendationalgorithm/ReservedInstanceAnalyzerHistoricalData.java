package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.internal.$guava$.annotations.$VisibleForTesting;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.component.db.tables.records.ComputeTierTypeHourlyByWeekRecord;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This class provides functionality, which was originally in ReservedInstanceAnalyzer, and has been
 * refactored into this class to make JUnit testing easier.
 *
 * <p>This class is intended to provide the facade to the ComputeTierDemandStatsStore, or the historical
 * demand data.  This would allow the ReservedInstanceAnalyzer to not need the
 * ComputeTierDemandStatsStore.  Not all the functionality that uses the ComputeTierDemandStatsStore
 * has been moved out of ReservedInstanceAnalyzer to here.
 * </p>
 *
 * <p>The ReservedInstanceAnalyzer constructor takes many parameters, which are Autowired.  Therefore,
 * JUnit testing the ReservedInstanceAnalyzer is an Autowire mess.
 * For example, while trying to dependency inject the PriceTableStore using a bean defined in
 * PricingConfig, there are nine levels of exceptions culminating in a BeanInstantiationException
 * with the message "Failed to construct kafka consumer".
 * </p>
 */
@ThreadSafe
public class ReservedInstanceAnalyzerHistoricalData {

    private static final Logger logger = LogManager.getLogger();

    // An interface for obtaining historical demand data
    private final ComputeTierDemandStatsStore computeTierDemandStatsStore;

    /**
     * The size of the demand data for a {@link ReservedInstanceZonalContext} from the
     * instance_type_hourly_by_week table. The size is total number of hours per week: 168 = 24 * 7.
     */
    private static final int WEEKLY_DEMAND_DATA_SIZE = 168;

    // Set the default value as -1.
    private static final int NO_DEMAND_DEFAULT_VALUE = -1;

    /**
     * Construct the historical demand data facade.
     *
     * @param computeTierDemandStatsStore historical demand data store.
     */
    public ReservedInstanceAnalyzerHistoricalData(ComputeTierDemandStatsStore computeTierDemandStatsStore) {
        this.computeTierDemandStatsStore = Objects.requireNonNull(computeTierDemandStatsStore);
    }

    /**
     * Because reserved instances (RIs) may be zonal or regional, and historical demand is stored
     * by zone, not region, this method determines the cluster of a region and the applicable zones
     * determined by the historical demand.  This allows analysis to determine the contexts to run
     * analysis on.
     * Given a set of database contexts, compute the clusters of database contexts, such that a
     * cluster is a region and set of zones associated with that region, and the clusters are within
     * scope.
     *
     * <p>NOTE: this was a private method that was moved to package accessible for JUnit testing.
     * TODO: currently, this method reads all the records from the historical data and computes the
     * clusters. The method should only read the database constexts, which will be a subset of the
     * records.
     * TODO: Computing the region associated with a zone is inefficient, and shoud be improved.
     * </p>
     *
     * @param scope What is allowed.  Used for testing.
     * @param computeTierFamilies mapping from family name to list of compute tiers in that family.
     * @param cloudTopology topology of cloud or dictionary of entities.
     * @return  map from ReservedInstanceRegionalContext to list of ReservedInstanceZonalContext
     * or null on error.
     * This can return an empty map on success when if there are no contexts needing analysis.
     */
    @$VisibleForTesting
    Map<ReservedInstanceRegionalContext,
        List<ReservedInstanceZonalContext>> computeAnalysisClusters(ReservedInstanceAnalysisScope scope,
                                                                    Map<String, List<TopologyEntityDTO>> computeTierFamilies,
                                                                    TopologyEntityCloudTopology cloudTopology) {
        // What are the historical contexts?
        // TODO: This should only get the contexts not every row in the table.
        Set<ComputeTierTypeHourlyByWeekRecord> riStatsRecords =
            computeTierDemandStatsStore.getAllDemandStats().collect(Collectors.toSet());
        // mapping from region context to the zonal contexts owned by this region.
        Map<ReservedInstanceRegionalContext, List<ReservedInstanceZonalContext>> map = new HashMap<>();

        for (ComputeTierTypeHourlyByWeekRecord riStatRecord : riStatsRecords) {
            long computeTierId = riStatRecord.getComputeTierId();
            long availabilityZoneId = riStatRecord.getAvailabilityZone();
            Optional<TopologyEntityDTO> computeTierDTO = cloudTopology.getEntity(computeTierId);
            if (!computeTierDTO.isPresent()) {
                logger.warn("ComputeTier {} not present in the real-time topology.", computeTierId);
                continue;
            }
            TopologyEntityDTO computeTier = computeTierDTO.get();

            // Improve efficiency of computing corresponding region
            Set<TopologyEntityDTO> connectedRegions = computeTier.getConnectedEntityListList()
                .stream()
                .filter(c -> c.getConnectedEntityType() == EntityType.REGION_VALUE)
                .map(c -> cloudTopology.getEntity(c.getConnectedEntityId()))
                .map(Optional::get)
                .collect(Collectors.toSet());
            Optional<TopologyEntityDTO> regionOpt = Optional.empty();
            for (TopologyEntityDTO r : connectedRegions) {
                if (r.getConnectedEntityListList().stream()
                    .anyMatch(c -> c.getConnectedEntityId() == availabilityZoneId)) {
                    regionOpt = Optional.of(r);
                    break;
                }
            }
            if (!regionOpt.isPresent()) {
                logger.debug("No region found to be associated with compute tier {} in availability zone id {}",
                    computeTier, availabilityZoneId);
                continue;
            }
            TopologyEntityDTO region = regionOpt.get();
            // if regions is empty, check all regions.
            if (!scope.getRegions().isEmpty() && !scope.getRegions().stream().anyMatch(id -> region.getOid() == id)) {
                logger.debug("region UUID {} not in scope {}", region.getOid(), scope.getRegions());
                continue;
            }

            OSType platform = OSType.forNumber(riStatRecord.getPlatform());
            if (!scope.getPlatforms().contains(platform)) {
                logger.debug("platform {} not in scope {} ", platform, scope.getPlatforms());
                continue;
            }

            Tenancy tenancy = Tenancy.forNumber(riStatRecord.getTenancy());
            if (!scope.getTenancies().contains(tenancy)) {
                logger.debug("tenancy {} not in scope ", tenancy, scope.getTenancies());
                continue;
            }

            long masterAccount = riStatRecord.getAccountId();
            if (scope.getAccounts() != null
                && scope.getAccounts().size() > 0 && !scope.getAccounts().contains(masterAccount)) {
                logger.debug("master account {} not in scope ", masterAccount, scope.getAccounts());
                continue;
            }

            ReservedInstanceZonalContext zonalContext =
                new ReservedInstanceZonalContext(
                    masterAccount,
                    platform,
                    tenancy,
                    computeTier,
                    availabilityZoneId);

            ReservedInstanceRegionalContext regionalContext;
            // for instance size flexible, find smallest instance type in family
            computeTier = getComputeTierToBuyFor(computeTier, platform, tenancy, computeTierFamilies);
            regionalContext = new ReservedInstanceRegionalContext(masterAccount, platform, tenancy,
                computeTier, region.getOid());

            List<ReservedInstanceZonalContext> contexts = map.get(regionalContext);
            if (contexts == null) {
                map.put(regionalContext, new ArrayList<>(Arrays.asList(zonalContext)));
            } else {
                if (!contexts.contains(zonalContext)) {
                    contexts.add(zonalContext);
                }
            }
        }
        return map;
    }

    /**
     * Get demand array from historical demand stats records from the database.
     *
     * @param scope RI analysis scope.
     * @param context RI Zonal context.
     * @param demandType name of the field that should be used to get demand data.
     * @return Demand array from weekly or monthly records from the database.
     */
    @Nonnull
    float[] getDemand(ReservedInstanceAnalysisScope scope,
                             ReservedInstanceZonalContext context,
                             ReservedInstanceHistoricalDemandDataType demandType) {

        Set<Long> accountNumbers = scope.getAccounts();
        final List<ComputeTierTypeHourlyByWeekRecord> records =
            computeTierDemandStatsStore.fetchDemandStats(context, accountNumbers);
        // Create a demand data array with the size of the weekly demand data size which is 168 = 24 * 7
        return getDemandFromRecords(records, demandType);
    }

    /**
     * Get demand array from demand stats records from the database.
     *
     * @param records Demand stats records from the database.
     * @param demandDataType Type of the demand stats.
     * @return Demand stats array
     */
    @Nonnull
    @VisibleForTesting
    float[] getDemandFromRecords(List<ComputeTierTypeHourlyByWeekRecord> records,
                                         ReservedInstanceHistoricalDemandDataType demandDataType) {

        float[] demands = new float[WEEKLY_DEMAND_DATA_SIZE];
        Arrays.fill(demands, NO_DEMAND_DEFAULT_VALUE);
        for (ComputeTierTypeHourlyByWeekRecord record : records) {
            int index = record.getHour() + (record.getDay() - 1) * 24;
            demands[index] =
                demandDataType.getDbFieldName().equals(
                        ReservedInstanceHistoricalDemandDataType.ALLOCATION.getDbFieldName()) ?
                        record.getCountFromSourceTopology().floatValue()
                    : record.getCountFromProjectedTopology().floatValue();
        }
        return demands;
    }

    /**
     * Given a cluster and the instance types sorted by coupons by family, return the computeTiers to buy RIs.
     * If instance size flexible, return the instance type in the family with the least coupons, otherwise return
     * the cluster's computeTier.
     *
     * @param profile the profile buying RIs for
     * @param platform what is the OS? e.g. LINUX or WINDOWS
     * @param tenancy how are underlying resources used? e.g. DEFAULT (shared), DEDICATED
     * @param computeTierFamilies sorted, by coupons, computeTiers by family
     * @return profile buying RIs for.  If compute tier flexible, compute tier in family with least coupons.
     */
    @VisibleForTesting
    @Nonnull
    TopologyEntityDTO getComputeTierToBuyFor(@Nonnull TopologyEntityDTO profile,
                                             OSType platform,
                                             Tenancy tenancy,
                                             Map<String, List<TopologyEntityDTO>> computeTierFamilies) {

        boolean isComputeTierFlexible = platform.equals(OSType.LINUX) &&
            (tenancy.equals(Tenancy.DEFAULT) || tenancy.equals(Tenancy.DEDICATED));
        if (!isComputeTierFlexible) {
            return profile;
        }
        String family = profile.getTypeSpecificInfo().getComputeTier().getFamily();
        List<TopologyEntityDTO> list = computeTierFamilies.get(family);
        if (list != null) {
            return list.get(0);
        }
        logger.error("Compute tier size flexible and no compute tier types for family={} in profile={} platform={} tenancy={}",
            family, profile.getDisplayName(), platform.name(), tenancy.name());
        return profile;
    }

    /**
     * All access to the computeTierDemandStatsStore is done in this class.
     * @return if contains over a week worth of data or not.
     */
    boolean containsDataOverWeek() {
        /**
         * TODO : Remove the hardcoded return true and uncomment the next line of code once
         * OM-50535 is implemented.
         */

        //return computeTierDemandStatsStore.containsDataOverWeek();
        return true;
    }

}

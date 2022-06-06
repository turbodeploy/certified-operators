package com.vmturbo.market.runner.wastedappserviceplans;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;

/**
 * Performs wasted Azure App Service Plan (ASP) analysis on topologies.
 *
 * <p/>See:
 * {@link WastedAppServicePlanAnalysisEngine#analyzeWastedAppServicePlans(TopologyInfo, Map,
 * TopologyCostCalculator, CloudTopology)}.
 */
public class WastedAppServicePlanAnalysisEngine {
    private final Logger logger = LogManager.getLogger();
    /**
     * Key of an Azure App Service Plan Linux License (internal representation).
     */
    private static final String LINUX = "Linux_AppServicePlan";
    /**
     * Key of an Azure App Service Plan Windows License (internal representation).
     */
    private static final String WINDOWS = "Windows_AppServicePlan";

    /**
     * Name of Entity Property for Azure App Service Plans that contains the number of applications
     * running on the plan.
     */
    private static final String TOTAL_APP_COUNT = "Total App Count";

    /**
     * Verify that a topology entity is an App Service Plan.
     *
     * @param candidateAppServicePlan candidate entity.
     * @return true if entity is app service plan
     */
    private boolean isAppServicePlan(TopologyEntityDTO candidateAppServicePlan) {
        // Verify is App Service Plan by checking if the license commodity is for an ASP which can be "Windows_AppServicePlan" or "Linux_AppServicePlan"
        Optional<CommoditiesBoughtFromProvider> commoditiesBoughtFromProvider =
                candidateAppServicePlan.getCommoditiesBoughtFromProvidersList()
                        .stream()
                        .filter(bought -> bought.getProviderEntityType()
                                == EntityType.COMPUTE_TIER_VALUE)
                        .findFirst();
        if (commoditiesBoughtFromProvider.isPresent()) {
            Optional<CommodityBoughtDTO> license =
                    commoditiesBoughtFromProvider.get().getCommodityBoughtList().stream().filter(
                            commodityBoughtDTO -> commodityBoughtDTO.getCommodityType().getType()
                                    == CommodityType.LICENSE_ACCESS_VALUE).findFirst();
            if (license.isPresent()) {
                String licenseKey = license.get().getCommodityType().getKey();
                return licenseKey.equals(LINUX) || licenseKey.equals(WINDOWS);
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * Collect all the Azure App Service Plans (ASPs) that are running no apps.
     *
     * @param candidateAppServicePlans list of candidate app service plans.
     * @return set of the UUIDs of all the ASPs in the environment that have no apps running on
     *         them.
     */
    private Set<Long> collectAllUnutilizedASPs(List<TopologyEntityDTO> candidateAppServicePlans) {
        // In the future, we will need to switch to streaming through the new model with VMSpecs (meaning GuestLoad check will be obsolete too).
        return candidateAppServicePlans.stream()
                .filter(topologyEntityDTO -> !topologyEntityDTO.getDisplayName()
                        .contains(SupplyChainConstants.GUEST_LOAD))
                .filter(this::isAppServicePlan)
                .filter(topologyEntityDTO -> topologyEntityDTO.getEntityPropertyMapMap() != null
                        && topologyEntityDTO.getEntityPropertyMapMap().containsKey(TOTAL_APP_COUNT)
                        && Integer.parseInt(
                        topologyEntityDTO.getEntityPropertyMapMap().get(TOTAL_APP_COUNT)) <= 0)
                .map(TopologyEntityDTO::getOid)
                .collect(Collectors.toSet());
    }

    /**
     * Perform the analysis on ASPs, generating delete actions for any App Service Plans (ASP) that
     * are wasted. Wasted ASPs are defined as ASPs that have no apps attached to them which means
     * they're
     * unnecessarily costing money and providing no function to customers.
     *
     * @param topologyInfo Information about the topology this analysis applies to.
     * @param topologyEntities The entities in the topology.
     * @param topologyCostCalculator {@link TopologyCostCalculator} for calculating cost of
     *         App Service Plans.
     * @param originalCloudTopology {@link CloudTopology} for calculating potential savings
     *         from
     *         deleting App Service Plans.
     * @return The {@link WastedAppServicePlanResults} object.
     */
    @Nonnull
    public WastedAppServicePlanResults analyzeWastedAppServicePlans(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final Map<Long, TopologyEntityDTO> topologyEntities,
            @Nonnull final TopologyCostCalculator topologyCostCalculator,
            @Nonnull final CloudTopology<TopologyEntityDTO> originalCloudTopology) {
        final String logPrefix = topologyInfo.getTopologyType() + " WastedAppServicePlanAnalysis "
                + topologyInfo.getTopologyContextId() + " with topology "
                + topologyInfo.getTopologyId() + " : ";

        logger.info("{} Started", logPrefix);
        final List<Action> actions;
        try {
            // Fetch entities and group by type to make it faster to use.
            // Enum value is lost so must use integer value for entity type.
            final Map<Integer, List<TopologyEntityDTO>> topologyEntitiesByEntityType =
                    topologyEntities.values().stream().collect(
                            Collectors.groupingBy(TopologyEntityDTO::getEntityType));

            // Find the app service plans that have no apps running on them. These are candidates for deletion.
            // Note that service and app component will migrate to app component spec and vmspec in the future for azure app service plans.
            Set<Long> unUtilizedAppServicePlans = collectAllUnutilizedASPs(
                    topologyEntitiesByEntityType.getOrDefault(
                            EntityType.APPLICATION_COMPONENT_VALUE, new ArrayList<>()));

            logger.info("Found " + unUtilizedAppServicePlans.size() + " Wasted App Service Plans");
            // Filter out non-controllable ASPs. Do not generate actions for controllable false.
            unUtilizedAppServicePlans = unUtilizedAppServicePlans.stream().filter(
                    id -> topologyEntities.get(id).getAnalysisSettings().getControllable()).collect(
                    Collectors.toSet());
            logger.info("Generating Actions for " + unUtilizedAppServicePlans.size()
                    + " Wasted App Service Plans (controllable)");

            // Generate actions based on wasted ASPs (activeASPs set should now contain only wasted ASP uuids).
            actions = unUtilizedAppServicePlans.stream().flatMap(
                    aspOid -> createActionsFromAppServicePlan(topologyEntities.get(aspOid),
                            topologyCostCalculator, originalCloudTopology).stream()).collect(
                    Collectors.toList());
            logger.info("{} Finished", logPrefix);
            return new WastedAppServicePlanResults(actions);
        } catch (RuntimeException e) {
            logger.debug(logPrefix + " error while running analysis " + e);
            logger.error(logPrefix + " error while running analysis");
            return WastedAppServicePlanResults.EMPTY;
        }
    }

    /**
     * Create a {@link Action.Builder} with a particular target.
     *
     * @param targetEntityOid id of the wasted ASP
     * @return {@link Action.Builder} with the common fields for the delete action populated
     */
    private Action.Builder newActionFromAppServicePlan(final long targetEntityOid,
            final Long sourceEntityOid) {
        final Delete.Builder deleteBuilder = Delete.newBuilder().setTarget(ActionEntity.newBuilder()
                .setId(targetEntityOid)
                .setType(EntityType.APPLICATION_COMPONENT_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD));

        // ASP Buys from compute tier so set that as source.
        if (sourceEntityOid != null) {
            deleteBuilder.setSource(ActionEntity.newBuilder()
                    .setId(sourceEntityOid)
                    .setType(EntityType.COMPUTE_TIER_VALUE)
                    .setEnvironmentType(EnvironmentType.CLOUD));
        }

        // TODO: Set executable to true when policy work is ready https://vmturbo.atlassian.net/browse/OM-85048
        final Action.Builder action = Action.newBuilder()
                // Assign a unique ID to each generated action.
                .setId(IdentityGenerator.next())
                .setDeprecatedImportance(0.0D)
                .setExecutable(false)
                .setInfo(ActionInfo.newBuilder().setDelete(deleteBuilder));
        return action;
    }

    /**
     * Create zero or more wasted ASP actions from an ASP DTO.
     *
     * @param topologyCostCalculator The {@link TopologyCostCalculator} for this topology.
     * @param originalCloudTopology The {@link CloudTopology} for the input topology.
     * @return {@link Collection}{@link Action} based on the wasted file(s) associated
     *         with the volume.
     */
    private Collection<Action> createActionsFromAppServicePlan(
            final TopologyEntityDTO appServicePlan, TopologyCostCalculator topologyCostCalculator,
            CloudTopology<TopologyEntityDTO> originalCloudTopology) {
        Optional<CostJournal<TopologyEntityDTO>> costJournalOpt =
                topologyCostCalculator.calculateCostForEntity(originalCloudTopology,
                        appServicePlan);

        double costSavings = 0.0d;
        if (costJournalOpt.isPresent()) {
            // This will set the hourly saving rate to the action
            costSavings = costJournalOpt.get().getTotalHourlyCost().getValue();
        } else {
            logger.debug("Unable to get cost for App Service Plan {}",
                    appServicePlan.getDisplayName());
        }
        // Fetch the ComputeTier provider ID
        Optional<Long> computeTierProviderId =
                TopologyDTOUtil.getAppServicePlanComputeTierProviderID(appServicePlan);
        if (!Objects.requireNonNull(computeTierProviderId).isPresent()) {
            // No actions if we don't have a ComputeTier.
            return Collections.emptyList();
        }
        // Application Component will migrate to VMSpec at a later date for App Service Plans.
        return Collections.singletonList(newActionFromAppServicePlan(appServicePlan.getOid(),
                computeTierProviderId.get()).setExplanation(
                        Explanation.newBuilder().setDelete(DeleteExplanation.newBuilder()))
                .setSavingsPerHour(CurrencyAmount.newBuilder().setAmount(costSavings))
                .build());
    }
}

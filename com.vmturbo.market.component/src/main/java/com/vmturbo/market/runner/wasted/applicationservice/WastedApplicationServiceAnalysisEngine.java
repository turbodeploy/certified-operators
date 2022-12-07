package com.vmturbo.market.runner.wasted.applicationservice;

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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

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
import com.vmturbo.market.runner.wasted.WastedEntityAnalysisEngine;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;

/**
 * Performs wasted Azure App Service Plan (ASP) analysis on topologies.
 *
 * <p/>See:
 * {@link WastedApplicationServiceAnalysisEngine#analyze(TopologyInfo, Map,
 * TopologyCostCalculator, CloudTopology)}.
 */
public class WastedApplicationServiceAnalysisEngine implements WastedEntityAnalysisEngine {
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
     * Verify that a topology entity is an App Service Plan.
     *
     * @param candidateAppServicePlan candidate entity.
     * @return true if entity is app service plan
     */
    public static boolean isAppServicePlan(TopologyEntityDTO candidateAppServicePlan) {
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
            }
        }
        return false;
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
                .filter(WastedApplicationServiceAnalysisEngine::isAppServicePlan)
                .filter(topologyEntityDTO -> topologyEntityDTO.hasTypeSpecificInfo()
                        && topologyEntityDTO.getTypeSpecificInfo().hasApplicationService()
                        && topologyEntityDTO.getTypeSpecificInfo().getApplicationService().getAppCount() <= 0)
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
     * @return The {@link WastedApplicationServiceResults} object.
     */
    @Nonnull
    @Override
    public WastedApplicationServiceResults analyze(
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
            // Note that service and app component will migrate to app component spec and Virtual Machine Spec in the future for azure app service plans.
            Collection<TopologyEntityDTO> listOfApplicationComponent = topologyEntitiesByEntityType.getOrDefault(
                    EntityType.APPLICATION_COMPONENT_VALUE, new ArrayList<>());
            Collection<TopologyEntityDTO> listOfVirtualMachineSpec = topologyEntitiesByEntityType.getOrDefault(
                    EntityType.VIRTUAL_MACHINE_SPEC_VALUE, new ArrayList<>());
            Set<Long> unUtilizedAppServicePlans = collectAllUnutilizedASPs(
                    Lists.newArrayList(Iterables.concat(listOfVirtualMachineSpec, listOfApplicationComponent)));

            logger.info("Found " + unUtilizedAppServicePlans.size() + " Wasted App Service Plans");
            unUtilizedAppServicePlans = unUtilizedAppServicePlans.stream()
                    // Filter out non-controllable ASPs. Do not generate actions for controllable false.
                    .filter(id -> topologyEntities.get(id).getAnalysisSettings().getControllable())
                    // Also make sure the policy controlling delete actions are enabled
                    .filter(id -> topologyEntities.get(id).getAnalysisSettings().getDeletable())
                    .collect(Collectors.toSet());
            logger.info("Generating Actions for " + unUtilizedAppServicePlans.size()
                    + " Wasted App Service Plans (controllable)");

            // Generate actions based on wasted ASPs (activeASPs set should now contain only wasted ASP uuids).
            actions = unUtilizedAppServicePlans.stream().flatMap(
                    aspOid -> createActionsFromAppServicePlan(topologyEntities.get(aspOid),
                            topologyCostCalculator, originalCloudTopology).stream()).collect(
                    Collectors.toList());
            logger.info("{} Finished", logPrefix);
            return new WastedApplicationServiceResults(actions);
        } catch (RuntimeException e) {
            logger.debug(logPrefix + " error while running analysis " + e);
            logger.error(logPrefix + " error while running analysis");
            return WastedApplicationServiceResults.EMPTY;
        }
    }

    /**
     * Create a {@link Action.Builder} with a particular target.
     *
     * @param targetEntity the wasted ASP
     * @param sourceEntityOid compute tier of ASP.
     * @return {@link Action.Builder} with the common fields for the delete action populated
     */
    private Action.Builder newActionFromAppServicePlan(final TopologyEntityDTO targetEntity,
            final Long sourceEntityOid) {
        final Delete.Builder deleteBuilder = Delete.newBuilder().setTarget(ActionEntity.newBuilder()
                .setId(targetEntity.getOid())
                .setType(targetEntity.getEntityType())
                .setEnvironmentType(EnvironmentType.CLOUD));

        // ASP Buys from compute tier so set that as source.
        if (sourceEntityOid != null) {
            deleteBuilder.setSource(ActionEntity.newBuilder()
                    .setId(sourceEntityOid)
                    .setType(targetEntity.getEntityType())
                    .setEnvironmentType(EnvironmentType.CLOUD));
        }

        return Action.newBuilder()
                // Assign a unique ID to each generated action.
                .setId(IdentityGenerator.next())
                .setDeprecatedImportance(0.0D)
                .setExecutable(true)
                .setInfo(ActionInfo.newBuilder().setDelete(deleteBuilder));
    }

    /**
     * Create zero or more wasted ASP actions from an ASP DTO.
     *
     * @param appServicePlan current Application service based entityDTO.
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
            logger.debug("Unable to get cost for Wasted App Service Plan {}",
                    appServicePlan.getDisplayName());
        }
        // Fetch the ComputeTier provider ID
        Optional<Long> computeTierProviderId =
                TopologyDTOUtil.getAppServicePlanComputeTierProviderID(appServicePlan);
        if (!Objects.requireNonNull(computeTierProviderId).isPresent()) {
            // No actions if we don't have a ComputeTier.
            return Collections.emptyList();
        }
        logger.debug("Generating delete action for Wasted App Service Plan {}",
                appServicePlan.getDisplayName());
        return Collections.singletonList(newActionFromAppServicePlan(appServicePlan,
                computeTierProviderId.get()).setExplanation(
                        Explanation.newBuilder().setDelete(DeleteExplanation.newBuilder()))
                .setSavingsPerHour(CurrencyAmount.newBuilder().setAmount(costSavings))
                .build());
    }
}

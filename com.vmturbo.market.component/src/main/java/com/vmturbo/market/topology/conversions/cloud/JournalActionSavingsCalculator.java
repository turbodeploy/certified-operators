package com.vmturbo.market.topology.conversions.cloud;

import static com.vmturbo.common.protobuf.topology.TopologyDTOUtil.ENTITY_WITH_ADDITIONAL_COMMODITY_CHANGES;
import static com.vmturbo.trax.Trax.trax;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MoreCollectors;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrBuilder;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverage;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.EntityUptime.EntityUptimeDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.journal.CostJournal.CostSourceFilter;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator.TraxSavingsDetails.TraxTierCostDetails;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommoditiesBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxCollectors;
import com.vmturbo.trax.TraxConfiguration.TraxContext;
import com.vmturbo.trax.TraxNumber;

/**
 * A {@link CloudActionSavingsCalculator} implementation based on {@link CostJournal} entity
 * calculations.
 */
public class JournalActionSavingsCalculator implements CloudActionSavingsCalculator {

    private final Logger logger = LogManager.getLogger();

    private final Map<Long, TopologyEntityDTO> sourceTopologyMap;

    private final CloudTopology<TopologyEntityDTO> sourceCloudTopology;

    private final TopologyCostCalculator sourceCostCalculator;

    //TODO This will be deprecated wen RIs are also migrated to teh cloud commitment framework.
    private final Map<Long, EntityReservedInstanceCoverage> projectedRICoverage;

    private final Map<Long, ProjectedTopologyEntity> projectedTopologyMap;

    private final Map<Long, CostJournal<TopologyEntityDTO>> projectedJournalsMap;

    private final Map<Long, Set<CloudCommitmentMapping>> projectedCommitmentCoverage;

    /**
     * Constructs a new {@link JournalActionSavingsCalculator} instance.
     * @param sourceTopologyMap The source topology map.
     * @param sourceCloudTopology The source cloud topology.
     * @param sourceCostCalculator The source cloud cost calculator.
     * @param projectedTopologyMap The projected topology map.
     * @param projectedJournalsMap The projected entity cost journals.
     * @param projectedRICoverage The projected RI coverage map.
     * @param projectedCommitmentCoverage The projected cloud commitment coverage.
     */
    public JournalActionSavingsCalculator(@Nonnull Map<Long, TopologyEntityDTO> sourceTopologyMap,
                                          @Nonnull CloudTopology<TopologyEntityDTO> sourceCloudTopology,
                                          @Nonnull TopologyCostCalculator sourceCostCalculator,
                                          @Nonnull Map<Long, ProjectedTopologyEntity> projectedTopologyMap,
                                          @Nonnull Map<Long, CostJournal<TopologyEntityDTO>> projectedJournalsMap,
                                          @Nonnull Map<Long, EntityReservedInstanceCoverage> projectedRICoverage,
                                          @Nonnull Map<Long, Set<CloudCommitmentMapping>> projectedCommitmentCoverage) {

        this.sourceTopologyMap = ImmutableMap.copyOf(Objects.requireNonNull(sourceTopologyMap));
        this.sourceCloudTopology = Objects.requireNonNull(sourceCloudTopology);
        this.sourceCostCalculator = Objects.requireNonNull(sourceCostCalculator);
        this.projectedTopologyMap = ImmutableMap.copyOf(Objects.requireNonNull(projectedTopologyMap));
        this.projectedJournalsMap = ImmutableMap.copyOf(Objects.requireNonNull(projectedJournalsMap));
        this.projectedRICoverage = ImmutableMap.copyOf(Objects.requireNonNull(projectedRICoverage));
        this.projectedCommitmentCoverage = ImmutableMap.copyOf(Objects.requireNonNull(projectedCommitmentCoverage));
    }


    /**
     * {@inheritDoc}.
     */
    @Override
    public CalculatedSavings calculateSavings(@Nonnull final ActionOrBuilder action) {

        final CalculatedSavings.Builder savingsBuilder = CalculatedSavings.builder();
        final ActionInfo actionInfo = action.getInfo();

        logger.trace("Calculating action savings for action {}:{}",
                actionInfo::getActionTypeCase, action::getId);

        try (TraxContext traxContext = Trax.track("SAVINGS", actionInfo.getActionTypeCase().name())) {
            switch (actionInfo.getActionTypeCase()) {

                case MOVE:
                    createSavingsDetailsForMove(actionInfo.getMove())
                            .ifPresent(details -> savingsBuilder
                                    .cloudSavingsDetails(details)
                                    .savingsPerHour(calculateSavingsFromDetails(details)));
                    break;
                case SCALE:
                    final Optional<TraxSavingsDetails> savingsDetails = createSavingsDetailsForScale(actionInfo.getScale());
                    savingsDetails.ifPresent(details -> savingsBuilder
                            .cloudSavingsDetails(details)
                            .savingsPerHour(calculateSavingsFromDetails(details)));
                    break;
                case ALLOCATE:
                    createSavingsDetailsForAllocate(actionInfo.getAllocate())
                            .ifPresent(details -> savingsBuilder
                                    .cloudSavingsDetails(details)
                                    .savingsPerHour(calculateSavingsFromDetails(details)));
                    break;
                case DEACTIVATE:
                    calculateDeactivateSavings(actionInfo.getDeactivate())
                            .ifPresent(savingsBuilder::savingsPerHour);
                    break;
                case PROVISION:
                    calculateProvisionSavings(actionInfo.getProvision())
                            .ifPresent(savingsBuilder::savingsPerHour);
                    break;
                default:
                    logger.debug("Action savings calculation not supported for action of type {}. Action is {}",
                            actionInfo::getActionTypeCase, action::toString);
                    break;

            }

            final CalculatedSavings calculatedSavings = savingsBuilder.build();
            if (traxContext.on() && calculatedSavings.savingsPerHour().isPresent()) {
                logger.info("{} calculation stack for {} action {}:\n{}",
                        () -> calculatedSavings.getClass().getSimpleName(),
                        () -> actionInfo.getActionTypeCase().name(),
                        () -> Long.toString(action.getId()),
                        () -> calculatedSavings.savingsPerHour().get().calculationStack());

            }

            return calculatedSavings;
        }
    }

    private Optional<TraxNumber> calculateDeactivateSavings(@Nonnull Deactivate deactivateAction) {
        long deactivatedEntityOid = deactivateAction.getTarget().getId();
        final ProjectedTopologyEntity deactivatedEntity =  projectedTopologyMap.get(deactivatedEntityOid);

        // get the entity DTO from the original topology
        final TopologyEntityDTO deactivatingEntity =  sourceTopologyMap.get(deactivatedEntityOid);

        if (ObjectUtils.allNotNull(deactivatingEntity, deactivatedEntity)) {
            if (!isCloudEntity(deactivatingEntity)) {
                return Optional.empty();
            }

            // get the cost journal of the entity from the source topology
            // since the entity in the projected topology is in suspended state
            // and does not have cost associated with it
            Optional<CostJournal<TopologyEntityDTO>> sourceCostJournal =
                    sourceCostCalculator.calculateCostForEntity(
                            sourceCloudTopology, deactivatingEntity);

            final TraxNumber savings = calculateHorizontalScalingActionSavings(
                    deactivatingEntity,
                    sourceCostJournal,
                    sourceCloudTopology.getTierProviders(deactivatedEntityOid),
                    true);

            return Optional.of(savings);
        }

        return Optional.empty();
    }

    private Optional<TraxNumber> calculateProvisionSavings(@Nonnull Provision provisionAction) {
        long modelSellerEntityOid = provisionAction.getEntityToClone().getId();

        // get the entity DTO from the original topology
        TopologyEntityDTO modelSellerEntity = sourceTopologyMap.get(modelSellerEntityOid);

        if (modelSellerEntity != null) {
            if (!isCloudEntity(modelSellerEntity)) {
                return Optional.empty();
            }

            // get the cost journal of the model seller
            // since the costs for the provisioned (cloned) entity is not computed
            final CostJournal<TopologyEntityDTO> modelSellerProjectedCostJournal
                    = projectedJournalsMap.get(modelSellerEntityOid);

            // If cost for the entity from the projected topology is not available
            if (!hasOnDemandCostForMarketTier(modelSellerProjectedCostJournal)) {
                // OM-59809 - we are seeing suspend and provision actions
                // for the same VM in realtime and plan topologies.
                // A VM that was suspended in the projected topology gets provisioned
                // during the analysis cycle. But since the VM was suspended originally
                // the cost for this suspended VM is not computed and the calculated savings could be 0
                Optional<CostJournal<TopologyEntityDTO>> entityCostJournalFromRealTimeTopology =
                        sourceCostCalculator.calculateCostForEntity(
                                sourceCloudTopology, modelSellerEntity);

                final TraxNumber horizontalScalingCost = calculateHorizontalScalingActionSavings(
                        modelSellerEntity,
                        entityCostJournalFromRealTimeTopology,
                        sourceCloudTopology.getTierProviders(modelSellerEntityOid),
                        false);

                logger.debug("{}: cost for entity in projected topology is not found, computed cost for entity in real time topology {}",
                        modelSellerEntity.getDisplayName(), horizontalScalingCost);
                return Optional.of(horizontalScalingCost);
            } else {
                final TraxNumber horizontalScalingCost = calculateHorizontalScalingActionSavings(
                        modelSellerEntity,
                        Optional.of(modelSellerProjectedCostJournal),
                        sourceCloudTopology.getTierProviders(modelSellerEntityOid),
                        false);

                return Optional.of(horizontalScalingCost);
            }
        }

        return Optional.empty();
    }

    private boolean isCloudEntity(@Nonnull final TopologyEntityDTO entity) {
        return entity.getEnvironmentType() == EnvironmentType.CLOUD;
    }

    private TraxNumber calculateHorizontalScalingActionSavings(
            @Nonnull TopologyEntityDTO cloudEntityHzScaling,
            @Nonnull Optional<CostJournal<TopologyEntityDTO>> entityCostJournal,
            @Nonnull Set<TopologyEntityDTO> tierProviders,
            boolean isSuspend) {
        if (!entityCostJournal.isPresent()) {
            return trax(0, "no entity cost journal");
        }

        CostJournal<TopologyEntityDTO> costJournal = entityCostJournal.get();
        if (!hasOnDemandCostForMarketTier(costJournal)) {
            return trax(0, "on-demand cost not computed");
        }

        // get cost for the compute tier only
        final TraxNumber costs = tierProviders.stream()
                .filter(tier -> TopologyDTOUtil.isPrimaryTierEntityType(tier.getEntityType()))
                .map(tier -> getOnDemandCostForMarketTier(cloudEntityHzScaling, tier, costJournal, false))
                .collect(TraxCollectors.sum("horizontal-scale-vm-in-cloud"));

        final String savingsDescription = String.format("%s for %s \"%s\" (%d)",
                isSuspend ? "Savings" : "Investment",
                EntityType.forNumber(cloudEntityHzScaling.getEntityType()).name(),
                cloudEntityHzScaling.getDisplayName(),
                cloudEntityHzScaling.getOid());

        // accumulated cost
        final TraxNumber savings = costs.times(isSuspend ? 1.0 : -1.0).compute(savingsDescription);
        return savings;
    }


    private Optional<TraxSavingsDetails> createSavingsDetailsForScale(@Nonnull Scale scaleAction) {

        final TopologyEntityDTO targetEntity = sourceTopologyMap.get(scaleAction.getTarget().getId());

        final Optional<ChangeProvider> tierChange = getCloudTierChangeProvider(scaleAction.getChangesList());
        if (tierChange.isPresent()) {

            final TopologyEntityDTO sourceTier = sourceTopologyMap.get(tierChange.get().getSource().getId());
            final ProjectedTopologyEntity destinationTier = projectedTopologyMap.get(tierChange.get().getDestination().getId());

            return createDetailsForTierChange(targetEntity, sourceTier, destinationTier.getEntity(), false);
        } else if (!scaleAction.getCommodityResizesList().isEmpty()) {

            final TopologyEntityDTO primaryProvider = sourceTopologyMap.get(scaleAction.getPrimaryProvider().getId());

            // A commodity change is treated as a tier change to the same tier.
            return createDetailsForTierChange(targetEntity, primaryProvider, primaryProvider, false);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Calculates savings details for MOVE actions. Savings details assume this is an MPC move action,
     * either across cloud targets or from on-prem to cloud. If the move is from on-prem to cloud,
     * only the destination data will be set for the details.
     * @param moveAction The target move action.
     * @return The savings details, representing the cloud tier change contained within the move action.
     */
    private Optional<TraxSavingsDetails> createSavingsDetailsForMove(@Nonnull Move moveAction) {
        final Optional<ChangeProvider> tierChange = getCloudTierChangeProvider(moveAction.getChangesList());
        final TopologyEntityDTO targetEntity;

        if (tierChange.isPresent()) {
            // For volume move action, there is `resource` in `tierChange`,
            // id of the disk can be retrieved in the `resource`,
            // `targetEntity` here should be the disk.
            // Otherwise, the `tierChange` can be the vm.
            if (tierChange.get().getResourceCount() == 1) {
                targetEntity = sourceTopologyMap.get(tierChange.get().getResource(0).getId());
            } else {
                targetEntity = sourceTopologyMap.get(moveAction.getTarget().getId());
            }

            final TopologyEntityDTO sourceTier = sourceTopologyMap.get(tierChange.get().getSource().getId());
            final ProjectedTopologyEntity destinationTier = projectedTopologyMap.get(tierChange.get().getDestination().getId());

            return createDetailsForTierChange(targetEntity, sourceTier, destinationTier.getEntity(), false);
        } else {
            return Optional.empty();
        }
    }

    private Optional<TraxSavingsDetails> createSavingsDetailsForAllocate(@Nonnull Allocate allocateAction) {

        final TopologyEntityDTO entityMoving = sourceTopologyMap.get(allocateAction.getTarget().getId());
        final TopologyEntityDTO sourceTier = sourceTopologyMap.get(allocateAction.getWorkloadTier().getId());
        final ProjectedTopologyEntity destinationTier = projectedTopologyMap.get(allocateAction.getWorkloadTier().getId());

        return createDetailsForTierChange(
                entityMoving, sourceTier, destinationTier.getEntity(),
                allocateAction.getIsBuyRecommendationCoverage());
    }

    private Optional<TraxSavingsDetails> createDetailsForTierChange(@Nullable TopologyEntityDTO entityMoving,
                                                                    @Nullable TopologyEntityDTO sourceTier,
                                                                    @Nullable TopologyEntityDTO destinationTier,
                                                                    boolean includeBuyRICoverage) {
        if (ObjectUtils.allNotNull(entityMoving, destinationTier)) {

            final TraxSavingsDetails.Builder cloudCostSavingsDetails = TraxSavingsDetails.builder();

            // TierCostDetails for destination/projected
            final CostJournal<TopologyEntityDTO> destCostJournal = projectedJournalsMap.get(
                    entityMoving.getOid());
            final EntityReservedInstanceCoverage projectedCoverage = projectedRICoverage.get(
                    entityMoving.getOid());
            final Set<CloudCommitmentMapping> commitmentMappings = projectedCommitmentCoverage.getOrDefault(entityMoving.getOid(),
                    Collections.emptySet());

            final TraxTierCostDetails projectedTierCostDetails = buildTierCostDetails(
                    destCostJournal,
                    entityMoving,
                    destinationTier,
                    projectedCoverage != null ? Optional.ofNullable(projectedCoverage) : Optional.empty(),
                    includeBuyRICoverage, commitmentMappings);
            cloudCostSavingsDetails.projectedTierCostDetails(projectedTierCostDetails);

            // TierCostDetails for source
            if (sourceTier != null) {
                final Optional<CostJournal<TopologyEntityDTO>> sourceCostJournal = sourceCostCalculator.calculateCostForEntity(
                        sourceCloudTopology,
                        entityMoving);
                final Optional<EntityReservedInstanceCoverage> originalCoverage = sourceCostCalculator.getCloudCostData()
                        .getFilteredRiCoverage(entityMoving.getOid());

                Set sourceCommitmentMappings = sourceCostCalculator.getCloudCostData()
                        .getCloudCommitmentMappingByEntityId()
                        .get(entityMoving.getOid());
                if (sourceCostJournal != null && sourceCostJournal.isPresent()) {
                    final TraxTierCostDetails sourceTierCostDetails = buildTierCostDetails(
                            sourceCostJournal.get(),
                            entityMoving,
                            sourceTier,
                            originalCoverage,
                            includeBuyRICoverage, sourceCommitmentMappings);
                    cloudCostSavingsDetails.sourceTierCostDetails(sourceTierCostDetails);
                }
            }

            //entity uptime of the cloudEntityMoving.
            if (destinationTier.getEntityType() == EntityType.COMPUTE_TIER_VALUE) {

                final EntityUptimeDTO entityUptime = sourceCostCalculator.getCloudCostData()
                        .getEntityUptime(entityMoving.getOid());
                if (entityUptime != null) {
                    cloudCostSavingsDetails.entityUptime(entityUptime);
                }
            }

            return Optional.of(cloudCostSavingsDetails.build());
        }

        return Optional.empty();
    }

    private Optional<ChangeProvider> getCloudTierChangeProvider(@Nonnull List<ChangeProvider> changeProviders) {

        return changeProviders.stream()
                .filter(changeProvider -> CloudTopology.CLOUD_TIER_TYPES.contains(changeProvider.getDestination().getType()))
                .collect(MoreCollectors.toOptional());
    }

    /**
     * Create the TierCostDetails for the projected and source marketTier.
     *
     * @param costJournal the cost journal.
     * @param entityMoving The entity moving.
     * @param cloudTier A cloud tier related to the source or destination of {@code entityMoving}.
     * @param reservedInstanceCoverage the reserved instance coverage of the cloudEntityMoving
     * @param includeBuyRICoverage Whether to include savings from buy RI recommendations.
     *
     * @return {@link TraxTierCostDetails} representing the TierCostDetails
     */
    private TraxTierCostDetails buildTierCostDetails(@Nullable CostJournal<TopologyEntityDTO> costJournal,
                                                     @Nonnull TopologyEntityDTO entityMoving,
                                                     @Nonnull TopologyEntityDTO cloudTier,
                                                     @Nonnull Optional<EntityReservedInstanceCoverage> reservedInstanceCoverage,
                                                     boolean includeBuyRICoverage,
                                                     @Nonnull Set<CloudCommitmentMapping> commitmentMappings) {
        if (costJournal == null) {
            return TraxTierCostDetails.EMPTY_DETAILS;
        }
        final TraxNumber onDemandCost = getOnDemandCostForMarketTier(
                entityMoving, cloudTier, costJournal, includeBuyRICoverage);
        final TraxNumber onDemandRate;
        if (cloudTier.getEntityType() == EntityType.STORAGE_TIER_VALUE) {
            onDemandRate = onDemandCost;
        } else {
            TraxNumber compute = costJournal.getHourlyCostFilterEntries(
                    CostCategory.ON_DEMAND_COMPUTE, CostSourceFilter.ON_DEMAND_RATE);
            TraxNumber license =  costJournal.getHourlyCostFilterEntries(
                    CostCategory.ON_DEMAND_LICENSE, CostSourceFilter.ON_DEMAND_RATE);
            onDemandRate = Stream.of(compute, license)
                    .collect(TraxCollectors.sum(cloudTier.getDisplayName() + "On-demand Cost"));
        }

        final TraxTierCostDetails.Builder tierCostDetails = TraxTierCostDetails.builder()
                .onDemandRate(onDemandRate)
                .onDemandCost(onDemandCost)
                .costJournal(costJournal);
        final CloudCommitmentCoverage.Builder cloudCommitmentCoverage =
                CloudCommitmentCoverage.newBuilder();
        // check on the commitment mappings first since source RI coverage has an entry of 0 coupons.
        if (reservedInstanceCoverage.isPresent()
                && (reservedInstanceCoverage.get().getCouponsCoveredByBuyRiCount() > 0
                        || reservedInstanceCoverage.get().getCouponsCoveredByRiCount() > 0)) {
            double coverageUsed = sumCoverage(reservedInstanceCoverage.get(), includeBuyRICoverage);
            cloudCommitmentCoverage
                    .setCapacity(CloudCommitmentAmount.newBuilder().setCoupons(reservedInstanceCoverage.get().getEntityCouponCapacity()))
                    .setUsed(CloudCommitmentAmount.newBuilder().setCoupons(coverageUsed));
        } else if (!commitmentMappings.isEmpty()) {

            // set based on costs - where the coverage is converted to a scalar value in the form of coupons.
            // Actual coverages will be set once the action details supports multiple commodity coverages.
            final CloudCommitmentAmount capacityInCoupons = CloudCommitmentAmount.newBuilder().setCoupons(1D).build();
            // used = scalar(amount discounted) / scalar(template capacity)
            // used as computed in SMA = ( net cost at 0 coverage - net cost at x coverage) / (net cost at 0 coverage - net cost at 100% coverage)
            // net cost at 0 coverage = compute/rate + license/rate
            // net cost at x coverage = compute/rate + license/rate + compute/CC discount + license/CC discount
            // net cost at 100% coverage is the license value since we do not have CC discounts on License, but, applies to only compute.
            TraxNumber onDemandCCDiscount =  costJournal.getFilteredCategoryCostsBySource(
                    CostCategory.ON_DEMAND_COMPUTE, CostSourceFilter.EXCLUDE_UPTIME).getOrDefault(
                    CostSource.CLOUD_COMMITMENT_DISCOUNT, Trax.trax(0D));
            TraxNumber onDemandLicenseCCDiscount =  costJournal.getFilteredCategoryCostsBySource(
                    CostCategory.ON_DEMAND_LICENSE, CostSourceFilter.EXCLUDE_UPTIME).getOrDefault(
                    CostSource.CLOUD_COMMITMENT_DISCOUNT, Trax.trax(0D));
            double netCostWithCoverage = Stream.of(onDemandRate, onDemandCCDiscount, onDemandLicenseCCDiscount)
                    .collect(TraxCollectors.sum(entityMoving.getDisplayName() + "net cost with CUD coverage")).getValue();
            double netCostWithFullCoverage = costJournal.getHourlyCostFilterEntries(
                    CostCategory.ON_DEMAND_LICENSE, CostSourceFilter.ON_DEMAND_RATE).getValue();
            double usedInCoupons = Math.abs(onDemandRate.getValue() - netCostWithCoverage) / (onDemandRate.getValue() - netCostWithFullCoverage);
            final CloudCommitmentAmount used = CloudCommitmentAmount.newBuilder().setCoupons(usedInCoupons).build();
            cloudCommitmentCoverage
                    .setCapacity(capacityInCoupons)
                    .setUsed(used);

        }
        tierCostDetails.cloudCommitmentCoverage(cloudCommitmentCoverage.build());
        if (cloudCommitmentCoverage.hasUsed()) {
            TraxNumber onDemandCompute = costJournal.getHourlyCostFilterEntries(
                    CostCategory.ON_DEMAND_COMPUTE, CostSourceFilter.EXCLUDE_UPTIME);
            TraxNumber onDemandLicense = costJournal.getHourlyCostFilterEntries(
                    CostCategory.ON_DEMAND_LICENSE, CostSourceFilter.EXCLUDE_UPTIME);
            TraxNumber reservedLicense = costJournal.getHourlyCostFilterEntries(
                    CostCategory.RESERVED_LICENSE, CostSourceFilter.EXCLUDE_UPTIME);
            final TraxNumber discountedRate = Stream.of(onDemandCompute, onDemandLicense, reservedLicense)
                    .collect(TraxCollectors.sum(cloudTier.getDisplayName() + " Discounted Rate"));
            tierCostDetails.discountedRate(discountedRate);
        }
        return tierCostDetails.build();
    }

    @Nonnull
    private  CloudCommitmentAmount getCommitmentAmountRequiredForTier(@Nonnull final TopologyEntityDTO tier) {
        return CloudCommitmentAmount.newBuilder().setCommoditiesBought(
                CommittedCommoditiesBought.newBuilder()
                        .addCommodity(CommittedCommodityBought.newBuilder()
                                .setCommodityType(CommodityType.NUM_VCORE)
                                .setCapacity(tier.getTypeSpecificInfo()
                                        .getComputeTier()
                                        .getNumOfCores()))
                        .addCommodity(CommittedCommodityBought.newBuilder()
                                .setCommodityType(CommodityType.MEM_PROVISIONED)
                                .setCapacity(tier.getCommoditySoldListList()
                                        .stream()
                                        .filter(c -> c.getCommodityType().getType()
                                                == CommodityType.MEM_PROVISIONED_VALUE)
                                        .findAny()
                                        .map(CommoditySoldDTO::getCapacity)
                                        .orElse(0D)))).build();
    }

    /**
     * Gets the cost for the market tier. In case of a compute/database market tier, it returns the
     * sum of compute + license + ip costs. In case of a storage market tier, it returns the
     * storage cost.
     *
     * @param cloudEntityMoving the entity moving.
     * @param cloudTier A cloud tier related to the source or destination of {@code cloudEntityMoving}.
     * @param journal the cost journal.
     * @param includeBuyRIDiscount Whether to include discounts from buy RI recommendations.
     * @return the total cost of the market tier
     */
    @Nonnull
    private TraxNumber getOnDemandCostForMarketTier(@Nonnull TopologyEntityDTO cloudEntityMoving,
                                                    @Nonnull TopologyEntityDTO cloudTier,
                                                    @Nonnull CostJournal<TopologyEntityDTO> journal,
                                                    boolean includeBuyRIDiscount) {
        final TraxNumber totalOnDemandCost;
        if (TopologyDTOUtil.isPrimaryTierEntityType(cloudTier.getEntityType())) {

            final CostSourceFilter costSourceFilter = includeBuyRIDiscount
                    ? CostSourceFilter.INCLUDE_ALL
                    : CostSourceFilter.EXCLUDE_BUY_RI_DISCOUNT_FILTER;

            // In determining on-demand costs for SCALE actions, the savings from buy RI actions
            // should be ignored. Therefore, we lookup the on-demand cost, ignoring savings
            // from CostSource.BUY_RI_DISCOUNT
            TraxNumber onDemandComputeCost = journal.getHourlyCostFilterEntries(
                    CostCategory.ON_DEMAND_COMPUTE,
                    costSourceFilter);
            TraxNumber licenseCost = journal.getHourlyCostFilterEntries(
                    CostCategory.ON_DEMAND_LICENSE,
                    costSourceFilter);
            TraxNumber reservedLicenseCost = journal.getHourlyCostFilterEntries(
                    CostCategory.RESERVED_LICENSE,
                    costSourceFilter);
            TraxNumber spotCost = journal.getHourlyCostForCategory(CostCategory.SPOT);
            if (spotCost == null) {
                spotCost = Trax.trax(0.0d);
            }

            // TODO Roop: remove this condition. OM-61424.
            TraxNumber dbStorageCost = ENTITY_WITH_ADDITIONAL_COMMODITY_CHANGES.contains(cloudEntityMoving.getEntityType())
                    ? journal.getHourlyCostFilterEntries(
                            CostCategory.STORAGE, costSourceFilter)
                    : Trax.trax(0.0);

            totalOnDemandCost = Stream.of(onDemandComputeCost, licenseCost, reservedLicenseCost, dbStorageCost, spotCost)
                    .collect(TraxCollectors.sum(cloudTier.getDisplayName() + " total cost"));
            logger.debug("Costs for {} on {} are -> on demand compute cost = {}, licenseCost = {},"
                            + " reservedLicenseCost = {}, dbStorageCost: {}, spotCost = {}",
                    cloudEntityMoving.getDisplayName(), cloudTier.getDisplayName(),
                    onDemandComputeCost, licenseCost, reservedLicenseCost, dbStorageCost, spotCost);
        } else {
            totalOnDemandCost = journal.getHourlyCostForCategory(CostCategory.STORAGE)
                    .named(cloudTier.getDisplayName() + " total cost");
            logger.debug("Costs for {} on {} are -> storage cost = {}",
                    cloudEntityMoving.getDisplayName(), cloudTier.getDisplayName(), totalOnDemandCost);
        }
        return totalOnDemandCost;
    }

    private double sumCoverage(@Nonnull EntityReservedInstanceCoverage entityReservedInstanceCoverage,
                               boolean includeBuyRICoverage) {
        final double aggregateInventoryCoverage = entityReservedInstanceCoverage.getCouponsCoveredByRiMap()
                .values()
                .stream()
                .mapToDouble(Double::doubleValue)
                .sum();

        final double aggregateBuyRICoverage;
        if (includeBuyRICoverage) {
            aggregateBuyRICoverage = entityReservedInstanceCoverage.getCouponsCoveredByBuyRiMap()
                    .values()
                    .stream()
                    .mapToDouble(Double::doubleValue)
                    .sum();
        } else {
            aggregateBuyRICoverage = 0.0;
        }

        return aggregateInventoryCoverage + aggregateBuyRICoverage;
    }

    private TraxNumber calculateSavingsFromDetails(@Nonnull TraxSavingsDetails savingsDetails) {

        final TraxNumber sourceOnDemandCost = savingsDetails.sourceTierCostDetails().onDemandCost();
        final TraxNumber destinationOnDemandCost = savingsDetails.projectedTierCostDetails().onDemandCost();

        return sourceOnDemandCost.minus(destinationOnDemandCost)
                .compute("Difference from source -> destination on-demand cost");

    }

    /**
     * Helper method to check if the on-demand compute cost for an entity is available.
     *
     * @param journal cost journal of an entity
     * @return Returns true if the on demand compute cost is available, else false
     */
    private boolean hasOnDemandCostForMarketTier(@Nullable CostJournal<TopologyEntityDTO> journal) {
        if (journal == null) {
            return false;
        }
        TraxNumber onDemandComputeCost = journal.getHourlyCostFilterEntries(
                CostCategory.ON_DEMAND_COMPUTE,
                CostSourceFilter.EXCLUDE_BUY_RI_DISCOUNT_FILTER);

        if (onDemandComputeCost.valueEquals(0.0)) {
            return false;
        }
        return true;
    }
}

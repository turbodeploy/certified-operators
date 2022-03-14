package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.vmturbo.cloud.common.commitment.CommitmentAmountCalculator;
import com.vmturbo.cloud.common.commitment.TopologyEntityCommitmentTopology;
import com.vmturbo.cloud.common.topology.SimulatedTopologyEntityCloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.CloudCommitmentApplicator;
import com.vmturbo.cost.calculation.CloudCostCalculator;
import com.vmturbo.cost.calculation.CloudCostCalculator.CloudCostCalculatorFactory;
import com.vmturbo.cost.calculation.CloudCostCalculator.DependentCostLookup;
import com.vmturbo.cost.calculation.PricingContext;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.journal.CostJournal.CostSourceFilter;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine.SortTemplateByOID;
import com.vmturbo.trax.Trax;

/**
 * All cost computation logic of SMA.
 */
public class SMACloudCostCalculator {

    /**
     * The cloud cost data.
     */
    private final CloudCostData<TopologyEntityDTO> cloudCostData;

    /**
     * Simulated cloud topology. This will have the cloud topology with simulation run on top of
     * it. The simulation will reflect some vms moving to a different compute tier.
     */

    private final SimulatedTopologyEntityCloudTopology simulatedTopologyEntityCloudTopology;

    /**
     * Look up table to save the cost already computed for a perticular costContext to improve
     * efficiency. This is also used to recreate the SMA run since the map is populated
     * before storing the diags.
     */
    private Map<CostContext, Float> cloudCostLookUp = new HashMap();

    /**
     * the cost calculator to compute various costs.
     */
    private final CloudCostCalculator<TopologyEntityDTO> costCalculator;


    public Map<CostContext, Float> getCloudCostLookUp() {
        return cloudCostLookUp;
    }

    /**
     * Constructor for SMACloudCostCalculator.
     *
     * @param cloudTopology the cloud topology.
     * @param cloudCostData the cloud cost data.
     */
    public SMACloudCostCalculator(SimulatedTopologyEntityCloudTopology cloudTopology,
            CloudCostData<TopologyEntityDTO> cloudCostData) {
        this.cloudCostData = cloudCostData;
        this.simulatedTopologyEntityCloudTopology = cloudTopology;
        costCalculator = createCostCalculator();
    }

    private CloudCostCalculator<TopologyEntityDTO> createCostCalculator() {
        CloudCostCalculatorFactory<TopologyEntityDTO> factory = CloudCostCalculator.newFactory();
        final Map<Long, CostJournal<TopologyEntityDTO>> retCosts = new HashMap<>(
                simulatedTopologyEntityCloudTopology.size());
        final DependentCostLookup<TopologyEntityDTO> dependentCostLookup = entity -> retCosts.get(
                entity.getOid());
        CloudCostCalculator<TopologyEntityDTO> costCalculator = factory.newCalculator(
                        cloudCostData,
                        simulatedTopologyEntityCloudTopology,
                        new TopologyEntityInfoExtractor(),
                        ReservedInstanceApplicator
                                        .newFactory(),
                        CloudCommitmentApplicator.newFactory(new TopologyEntityCommitmentTopology
                                        .TopologyEntityCommitmentTopologyFactory()),
                        dependentCostLookup,
                        new HashMap<>());
        return costCalculator;
    }

    /**
     * Calculate the cost journal for the entity if moved to computeTierId and covered by riId.
     *
     * @param entityId the entity id of the virtual machine
     * @param computeTierId The compute tier to which the virtual machine is moving to.
     * @param riCoverage The riCoverage of the VM.
     * @param riId the id of the ri covering the VM. considered only when riCoverage is > 0
     * @return the cost journal for the vm if moved to computeTierId and covered by riId
     */
    private CostJournal<TopologyEntityDTO> calculateCost(Long entityId,
            Long computeTierId, float riCoverage, Long riId) {
        Optional<TopologyEntityDTO> entity = simulatedTopologyEntityCloudTopology.getEntity(
                entityId);
        if (!entity.isPresent()) {
            return null;
        }
        Map<Long, Double> riCoverageMap = new HashMap<>();
        if (riCoverage > SMAUtils.EPSILON) {
            riCoverageMap.put(riId, (double)riCoverage);
        }
        Map<Long, Long> entityIdToComputeTierIdSimulation = new HashMap<>();
        entityIdToComputeTierIdSimulation.put(entityId, computeTierId);
        simulatedTopologyEntityCloudTopology.setEntityIdToComputeTierIdSimulation(entityIdToComputeTierIdSimulation);
        final Map<Long, EntityReservedInstanceCoverage> topologyRICoverage = new HashMap<>();
        if (!riCoverageMap.isEmpty()) {
            EntityReservedInstanceCoverage entityReservedInstanceCoverage =
                    EntityReservedInstanceCoverage.newBuilder()
                            .setEntityId(entityId)
                            .setEntityCouponCapacity(
                                    simulatedTopologyEntityCloudTopology.getRICoverageCapacityForEntity(
                                            entityId))
                            .putAllCouponsCoveredByRi(riCoverageMap)
                            .build();
            topologyRICoverage.put(entityId, entityReservedInstanceCoverage);
            costCalculator.setTopologyRICoverage(topologyRICoverage);
        }
        return costCalculator.calculateCost(entity.get());
    }

    /**
     * Compute the net cost for the virrtual machine if moved to the compute tier with id
     * computeTierId
     * and gets covered by ri with id riid and coverage riCoverage.
     *
     * @param virtualMachine the virtual machine of interest.
     * @param smaTemplate the compute tier the vm is moving to.
     * @param coverageAvailable the coverage available for the vm
     * @param riId the id of the ri discounting the vm
     * @return cost if moved to computeTierId with the specified coverage.
     */
    public float getNetCost(SMAVirtualMachine virtualMachine, SMATemplate smaTemplate,
            CloudCommitmentAmount coverageAvailable, Long riId) {
        Long computeTierId = smaTemplate.getOid();
        // TODO this has to be more generic to deal with GCP
        float riCoverage = (float)(CommitmentAmountCalculator.isStrictSuperSet(coverageAvailable, smaTemplate.getCommitmentAmount(), SMAUtils.EPSILON)
                ? smaTemplate.getCommitmentAmount().getCoupons() : coverageAvailable.getCoupons());
        PricingContext pricingContext = new PricingContext(virtualMachine.getRegionId(),
                virtualMachine.getOsType(), virtualMachine.getOsLicenseModel(), computeTierId,
                virtualMachine.getAccountPricingDataOid());
        CostContext context = new CostContext(pricingContext, riCoverage);

        Float savedCost = cloudCostLookUp.get(context);
        if (savedCost != null) {
            return savedCost;
        } else {
            savedCost = Float.MAX_VALUE;
        }
        long entityId = virtualMachine.getOid();
        CostJournal<TopologyEntityDTO> costJournal = calculateCost(entityId,
                computeTierId, riCoverage, riId);
        if (costJournal != null) {
            double onDemandCompute = costJournal.getFilteredCategoryCostsBySource(
                    CostCategory.ON_DEMAND_COMPUTE, CostSourceFilter.ON_DEMAND_RATE).getOrDefault(
                    CostSource.ON_DEMAND_RATE, Trax.trax(Double.MAX_VALUE)).getValue();
            double onDemandLicense = costJournal.getFilteredCategoryCostsBySource(
                    CostCategory.ON_DEMAND_LICENSE, CostSourceFilter.ON_DEMAND_RATE).getOrDefault(
                    CostSource.ON_DEMAND_RATE, Trax.trax(0d)).getValue();
            double discountCompute = costJournal.getFilteredCategoryCostsBySource(
                    CostCategory.ON_DEMAND_COMPUTE, CostSourceFilter.EXCLUDE_UPTIME).getOrDefault(
                    CostSource.RI_INVENTORY_DISCOUNT, Trax.trax(0d)).getValue();
            double discountLicense = costJournal.getFilteredCategoryCostsBySource(
                    CostCategory.ON_DEMAND_LICENSE, CostSourceFilter.EXCLUDE_UPTIME).getOrDefault(
                    CostSource.RI_INVENTORY_DISCOUNT, Trax.trax(0d)).getValue();
            if (onDemandCompute != Double.MAX_VALUE) {
                savedCost = (float)(onDemandCompute + onDemandLicense + discountCompute
                        + discountLicense);
            }
        }
        cloudCostLookUp.put(context, savedCost);
        return savedCost;
    }

    /**
     * Update the providers, group providers, natural template and the mincostprovider per family
     * for
     * each virtual machine.
     *
     * @param providers the list of providers
     * @param currentTemplate the current template of the vm
     * @param virtualMachine the vm of interest.
     * @return SMAVirtualMachineProvider datastructure which encapsulate all the computed values.
     */
    public SMAVirtualMachineProvider updateProvidersOfVirtualMachine(
            final List<SMATemplate> providers, SMATemplate currentTemplate,
            SMAVirtualMachine virtualMachine) {
        SMAVirtualMachineProvider smaVirtualMachineProvider = new SMAVirtualMachineProvider();
        if (providers == null || providers.isEmpty()) {
            if (currentTemplate != null) {
                smaVirtualMachineProvider = updateGroupProvidersOfVirtualMachine(
                        Arrays.asList(currentTemplate), virtualMachine, currentTemplate);
            }
        } else {
            // If currentTemplate has no cost data then the VM cannot move.
            // and currentTemplate is still valid template the vm can move to.
            // getOnDemandTotalCost returns Float.MAX_VALUE if there is no cost data.
            if (currentTemplate != null && providers.stream().anyMatch(
                    p -> p.getOid() == currentTemplate.getOid()) && (getNetCost(virtualMachine,
                    currentTemplate, CommitmentAmountCalculator.ZERO_COVERAGE, SMAUtils.UNKNOWN_OID) == Float.MAX_VALUE)) {
                smaVirtualMachineProvider = updateGroupProvidersOfVirtualMachine(
                        Arrays.asList(currentTemplate), virtualMachine, currentTemplate);
            } else {
                smaVirtualMachineProvider = updateGroupProvidersOfVirtualMachine(providers,
                        virtualMachine, currentTemplate);
                // If the natural template is giving infinite quote then stay in the current template.
                if (smaVirtualMachineProvider.getNaturalTemplate() != null) {
                    float naturalTemplateNetCost = getNetCost(virtualMachine,
                            smaVirtualMachineProvider.getNaturalTemplate(), CommitmentAmountCalculator.ZERO_COVERAGE,
                            SMAUtils.UNKNOWN_OID);
                    if (naturalTemplateNetCost == Float.MAX_VALUE) {
                        smaVirtualMachineProvider = updateGroupProvidersOfVirtualMachine(
                                Arrays.asList(currentTemplate), virtualMachine, currentTemplate);
                    }
                }
            }
        }
        smaVirtualMachineProvider.setProviders(providers);
        return smaVirtualMachineProvider;
    }

    /**
     * Update the group providers, natural template and the mincostprovider per family for
     * each virtual machine. This does not update the providers.
     *
     * @param groupProviders the list of groupproviders
     * @param currentTemplate the current template of the vm
     * @param virtualMachine the vm of interest.
     * @return SMAVirtualMachineProvider datastructure which encapsulate all the computed values.
     */
    public SMAVirtualMachineProvider updateGroupProvidersOfVirtualMachine(
            final List<SMATemplate> groupProviders, SMAVirtualMachine virtualMachine,
            SMATemplate currentTemplate) {
        SMAVirtualMachineProvider smaVirtualMachineProvider = new SMAVirtualMachineProvider();
        smaVirtualMachineProvider.setGroupProviders(groupProviders);
        updateNaturalTemplateAndMinCostProviderPerFamily(smaVirtualMachineProvider, virtualMachine,
                currentTemplate);
        return smaVirtualMachineProvider;
    }

    /**
     * Sets naturalTemplate: the natural (least cost) template.
     * Sets minCostProviderPerFamily: the least cost template on a per family basis.
     * Call only after templates have been processed fully.
     * smaVirtualMachineProvider gets updated as part of this method.
     *
     * @param smaVirtualMachineProvider datastructure which encapsulate all the computed
     *         values.
     * @param virtualMachine the vm of interest.
     * @param currentTemplate the current template of the vm
     */
    public void updateNaturalTemplateAndMinCostProviderPerFamily(
            SMAVirtualMachineProvider smaVirtualMachineProvider, SMAVirtualMachine virtualMachine,
            SMATemplate currentTemplate) {
        HashMap<String, SMATemplate> minCostProviderPerFamily = new HashMap<>();
        Optional<SMATemplate> naturalOptional = Optional.empty();
        List<SMATemplate> groupProviders = smaVirtualMachineProvider.getGroupProviders();
        Collections.sort(groupProviders, new SortTemplateByOID());

        /*
        order for natural template and mincost provider in family:
        ondemand cost
        penalty
        current template
        lower oid
        */

        for (SMATemplate template : groupProviders) {
            float onDemandTotalCost = getNetCost(virtualMachine, template, CommitmentAmountCalculator.ZERO_COVERAGE, SMAUtils.UNKNOWN_OID);
            float onDemandTotalCostWithPenalty = onDemandTotalCost + template.getScalingPenalty();
            if (!naturalOptional.isPresent()) {
                naturalOptional = Optional.of(template);
            } else {
                float naturalOnDemandTotalCost = getNetCost(virtualMachine, naturalOptional.get(),
                        CommitmentAmountCalculator.ZERO_COVERAGE, SMAUtils.UNKNOWN_OID);
                float naturalOnDemandTotalCostWithPenalty =
                        naturalOnDemandTotalCost + naturalOptional.get().getScalingPenalty();
                if (onDemandTotalCost - naturalOnDemandTotalCost < (-1 * SMAUtils.EPSILON)) {
                    // ondemand cost breaks the tie
                    naturalOptional = Optional.of(template);
                } else if (Math.abs(onDemandTotalCost - naturalOnDemandTotalCost) < SMAUtils.EPSILON
                        && onDemandTotalCostWithPenalty - naturalOnDemandTotalCostWithPenalty < (-1
                        * SMAUtils.EPSILON)) {
                    // ondemand cost the same. penalty breaks the tie.
                    naturalOptional = Optional.of(template);
                } else if (Math.abs(onDemandTotalCost - naturalOnDemandTotalCost) < SMAUtils.EPSILON
                        && Math.abs(
                        onDemandTotalCostWithPenalty - naturalOnDemandTotalCostWithPenalty)
                        < SMAUtils.EPSILON && (template.getOid() == currentTemplate.getOid())) {
                    // ondemand cost the same. penalty the same. current template breaks the tie.
                    naturalOptional = Optional.of(template);
                }
            }
            if (minCostProviderPerFamily.get(template.getFamily()) == null) {
                minCostProviderPerFamily.put(template.getFamily(), template);
            } else {
                float minCostProviderOnDemandTotalCost = getNetCost(virtualMachine,
                        minCostProviderPerFamily.get(template.getFamily()), CommitmentAmountCalculator.ZERO_COVERAGE,
                        SMAUtils.UNKNOWN_OID);
                float minCostProviderOnDemandTotalCostWithPenalty =
                        minCostProviderOnDemandTotalCost + minCostProviderPerFamily.get(
                                template.getFamily()).getScalingPenalty();
                if (onDemandTotalCost - minCostProviderOnDemandTotalCost < (-1
                        * SMAUtils.EPSILON)) {
                    // ondemand cost breaks the tie
                    minCostProviderPerFamily.put(template.getFamily(), template);
                } else if (Math.abs(onDemandTotalCost - minCostProviderOnDemandTotalCost)
                        < SMAUtils.EPSILON
                        && (onDemandTotalCostWithPenalty - minCostProviderOnDemandTotalCostWithPenalty)
                                < (-1 * SMAUtils.EPSILON)) {
                    // ondemand cost same. penality break the tie.
                    minCostProviderPerFamily.put(template.getFamily(), template);
                } else if (Math.abs(onDemandTotalCost - minCostProviderOnDemandTotalCost)
                        < SMAUtils.EPSILON && Math.abs(
                        onDemandTotalCostWithPenalty - minCostProviderOnDemandTotalCostWithPenalty)
                        < SMAUtils.EPSILON && (template.getOid() == currentTemplate.getOid())) {
                    // ondemand cost the same. penalty the same. current template breaks the tie.
                    minCostProviderPerFamily.put(template.getFamily(), template);
                }
            }
        }
        // If no minimum  is found, then use the current as the natural one
        smaVirtualMachineProvider.setNaturalTemplate(naturalOptional.orElse(currentTemplate));
        smaVirtualMachineProvider.setMinCostProviderPerFamily(minCostProviderPerFamily);
    }

    /**
     * Compute the savings obtained by matching the virtual machine with reservedInstance.
     *
     * @param vm virtual machine of interest.
     * @param virtualMachineGroupMap map from group name to virtualMachine Group
     * @param coupons remaining coupons available in the RI.
     * @param reservedInstance the reserved instance for which we compute the saving.
     * @return the savings obtained by matching the virtual machine with reservedInstance.
     */
    public float computeSaving(SMAVirtualMachine vm,
            Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap, CloudCommitmentAmount coupons,
            SMAReservedInstance reservedInstance) {
        final List<SMAVirtualMachine> vmList;
        if (vm.getGroupSize() > 1) {
            vmList = virtualMachineGroupMap.get(vm.getGroupName()).getVirtualMachines();
        } else {
            vmList = Collections.singletonList(vm);
        }
        SMATemplate riTemplate = reservedInstance.getNormalizedTemplate();
        float netSavingvm = 0;
        for (SMAVirtualMachine member : vmList) {
            float onDemandCostvm = getNetCost(vm, member.getNaturalTemplate(), CommitmentAmountCalculator.ZERO_COVERAGE,
                    SMAUtils.UNKNOWN_OID);
            /*  ISF : VM with t3.large and a ISF RI in t2 family. VM can move to t2.large.
             *  t2.large need 10 coupons. But we have only 6 coupons.
             *  riTemplate.getFamily() returns t2.
             *  getMinCostProviderPerFamily().get("t2") returns t2.large
             *  netCost will be ondemandCost * .4 + discountedCost 0.6 of the t2.large template.
             *  Note that for AWS the discountedCost will be 0;
             */
            /*  Non-ISF VM with t3.large and a RI is t2.large. VM has to move to t2.large.
             *  t2.large need 10 coupons.
             *  riTemplate returns t2.large.
             *  afterMoveCost will be ondemandCost * 0 + discountedCost * 1 of the t2.large template.
             *  Note that for AWS the discountedCost will be 0; So afterMoveCost will be 0;
             */
            float afterMoveCostvm = reservedInstance.isIsf() ? getNetCost(vm,
                    member.getMinCostProviderPerFamily(riTemplate.getFamily()),
                    CommitmentAmountCalculator.multiplyAmount(coupons, 1.0f / vm.getGroupSize()), reservedInstance.getOid()) : getNetCost(vm,
                    riTemplate, CommitmentAmountCalculator.multiplyAmount(coupons, 1.0f / vm.getGroupSize()), reservedInstance.getOid());
            netSavingvm += (onDemandCostvm - afterMoveCostvm);
        }
        return netSavingvm;
    }

    /**
     * compare the saving per coupon of two virtual machines when discounted by this RI.
     *
     * @param vm1 first VM
     * @param vm2 second VM
     * @param virtualMachineGroupMap map from group name to virtualMachine Group
     * @param coupons remaining coupons available in the RI.
     * @param reservedInstance the reserved instance for which we are comparing the cost.
     * @return -1 if vm1 has more saving per coupon. 1 if vm2 has more saving per coupon. 0
     *         otherwise.
     */
    public int compareCost(SMAVirtualMachine vm1, SMAVirtualMachine vm2,
            Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap, CloudCommitmentAmount coupons,
            SMAReservedInstance reservedInstance) {
        float netSavingVm1 = computeSaving(vm1, virtualMachineGroupMap, coupons, reservedInstance);
        float netSavingVm2 = computeSaving(vm2, virtualMachineGroupMap, coupons, reservedInstance);

        String riFamily = reservedInstance.getNormalizedTemplate().getFamily();
        CloudCommitmentAmount couponsVm1 = CommitmentAmountCalculator.ZERO_COVERAGE;
        CloudCommitmentAmount couponsVm2 = CommitmentAmountCalculator.ZERO_COVERAGE;
        if (reservedInstance.isIsf()) {
            couponsVm1 =
                    CommitmentAmountCalculator.min(coupons,
                            CommitmentAmountCalculator.multiplyAmount(vm1.getMinCostProviderPerFamily(riFamily).getCommitmentAmount(),
                                    vm1.getGroupSize()));
            couponsVm2 = CommitmentAmountCalculator.min(coupons,
                    CommitmentAmountCalculator.multiplyAmount(vm2.getMinCostProviderPerFamily(riFamily).getCommitmentAmount(), vm2.getGroupSize()));
        } else {
            couponsVm1 = CommitmentAmountCalculator.multiplyAmount(reservedInstance.getNormalizedTemplate().getCommitmentAmount(),
                    vm1.getGroupSize());
            couponsVm2 = CommitmentAmountCalculator.multiplyAmount(reservedInstance.getNormalizedTemplate().getCommitmentAmount(),
                    vm2.getGroupSize());
        }

        float netSavingVm1PerCoupon = netSavingVm1 / (float)couponsVm1.getCoupons();
        float netSavingVm2PerCoupon = netSavingVm2 / (float)couponsVm2.getCoupons();

        netSavingVm1PerCoupon = SMAUtils.round(netSavingVm1PerCoupon);
        netSavingVm2PerCoupon = SMAUtils.round(netSavingVm2PerCoupon);

        // Pick VM with higher savings per coupon.
        if ((netSavingVm1PerCoupon - netSavingVm2PerCoupon) > SMAUtils.EPSILON) {
            return -1;
        }
        if ((netSavingVm2PerCoupon - netSavingVm1PerCoupon) > SMAUtils.EPSILON) {
            return 1;
        }
        return 0;
    }
}

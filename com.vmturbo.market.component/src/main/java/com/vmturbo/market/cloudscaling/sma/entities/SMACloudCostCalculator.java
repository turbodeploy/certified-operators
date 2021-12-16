package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine.CostContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine.SortTemplateByOID;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * All cost computation logic of SMA.
 */
public class SMACloudCostCalculator {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The cloud topology. This is used to create the costCalculator.
     */
    private final CloudTopology<TopologyEntityDTO> cloudTopology;
    /**
     * The cloud cost data.
     */
    private final CloudCostData<TopologyEntityDTO> cloudCostData;

    /**
     * Constructor for SMACloudCostCalculator.
     */
    public SMACloudCostCalculator() {
        this.cloudTopology = null;
        this.cloudCostData = null;
    }

    public CloudCostData<TopologyEntityDTO> getCloudCostData() {
        return cloudCostData;
    }

    public CloudTopology<TopologyEntityDTO> getCloudTopology() {
        return cloudTopology;
    }

    /**
     * Update the providers, group providers, natural template and the mincostprovider per family for
     * each virtual machine.
     * @param providers the list of providers
     * @param currentTemplate the current template of the vm
     * @param costContext the costContext of the vm.
     * @return SMAVirtualMachineProvider datastructure which encapsulate all the computed values.
     */
    public SMAVirtualMachineProvider updateProvidersOfVirtualMachine(
            final List<SMATemplate> providers, SMATemplate currentTemplate, CostContext costContext) {
        SMAVirtualMachineProvider smaVirtualMachineProvider =
                new SMAVirtualMachineProvider();
        if (providers == null || providers.isEmpty()) {
            if (currentTemplate != null) {
                smaVirtualMachineProvider = updateGroupProvidersOfVirtualMachine(Arrays.asList(currentTemplate), costContext, currentTemplate);
            }
        } else {
            // If currentTemplate has no cost data then the VM cannot move.
            // and currentTemplate is still valid template the vm can move to.
            // getOnDemandTotalCost returns Float.MAX_VALUE if there is no cost data.
            if (currentTemplate != null
                    && providers.stream().anyMatch(p -> p.getOid() == currentTemplate.getOid())
                    && (getOnDemandTotalCost(costContext, currentTemplate) == Float.MAX_VALUE)) {
                smaVirtualMachineProvider = updateGroupProvidersOfVirtualMachine(Arrays.asList(currentTemplate), costContext, currentTemplate);
            } else {
                smaVirtualMachineProvider = updateGroupProvidersOfVirtualMachine(providers, costContext, currentTemplate);
                // If the natural template is giving infinite quote then stay in the current template.
                if (smaVirtualMachineProvider.getNaturalTemplate() != null
                        && (getOnDemandTotalCost(costContext, smaVirtualMachineProvider.getNaturalTemplate()) == Float.MAX_VALUE)) {
                    smaVirtualMachineProvider = updateGroupProvidersOfVirtualMachine(Arrays.asList(currentTemplate), costContext, currentTemplate);
                }
            }
        }
        smaVirtualMachineProvider.setProviders(providers);
        return smaVirtualMachineProvider;
    }

    /**
     * Update the group providers, natural template and the mincostprovider per family for
     * each virtual machine. This does not update the providers.
     * @param groupProviders the list of groupproviders
     * @param currentTemplate the current template of the vm
     * @param costContext the costContext of the vm.
     * @return SMAVirtualMachineProvider datastructure which encapsulate all the computed values.
     */
    public SMAVirtualMachineProvider updateGroupProvidersOfVirtualMachine(final List<SMATemplate> groupProviders,
        CostContext costContext, SMATemplate currentTemplate) {
        SMAVirtualMachineProvider smaVirtualMachineProvider = new SMAVirtualMachineProvider();
        smaVirtualMachineProvider.setGroupProviders(groupProviders);
        updateNaturalTemplateAndMinCostProviderPerFamily(smaVirtualMachineProvider, costContext, currentTemplate);
        return smaVirtualMachineProvider;
    }

    /**
     * Sets naturalTemplate: the natural (least cost) template.
     * Sets minCostProviderPerFamily: the least cost template on a per family basis.
     * Call only after templates have been processed fully.
     * smaVirtualMachineProvider gets updated as part of this method.
     *
     * @param smaVirtualMachineProvider datastructure which encapsulate all the computed values.
     * @param costContext the costContext of the vm.
     * @param currentTemplate the current template of the vm
     */
    public void updateNaturalTemplateAndMinCostProviderPerFamily(SMAVirtualMachineProvider smaVirtualMachineProvider,
            CostContext costContext, SMATemplate currentTemplate) {
        HashMap<String, SMATemplate> minCostProviderPerFamily = new HashMap<>();
        Optional<SMATemplate> naturalOptional = Optional.empty();
        List<SMATemplate> groupProviders = smaVirtualMachineProvider.getGroupProviders();
        Collections.sort(groupProviders, new SortTemplateByOID());

        /*
        order for natural template:
        ondemand cost
        penalty
        current template
        lower oid

        order for mincost provider in family:
        ondemand cost
        discounted cost
        penalty
        current template
        lower oid
         */

        for (SMATemplate template : groupProviders) {
            float onDemandTotalCost = getOnDemandTotalCost(costContext, template);
            float onDemandTotalCostWithPenalty = onDemandTotalCost + template.getScalingPenalty();
            if (!naturalOptional.isPresent()) {
                naturalOptional = Optional.of(template);
            } else {
                float naturalOnDemandTotalCost = getOnDemandTotalCost(costContext, naturalOptional.get());
                float naturalOnDemandTotalCostWithPenalty = naturalOnDemandTotalCost + naturalOptional.get().getScalingPenalty();
                if (onDemandTotalCost - naturalOnDemandTotalCost < (-1 * SMAUtils.EPSILON)) {
                    // ondemand cost breaks the tie
                    naturalOptional = Optional.of(template);
                } else if (Math.abs(onDemandTotalCost - naturalOnDemandTotalCost) < SMAUtils.EPSILON
                        && onDemandTotalCostWithPenalty - naturalOnDemandTotalCostWithPenalty < (-1 * SMAUtils.EPSILON)) {
                    // ondemand cost the same. penalty breaks the tie.
                    naturalOptional = Optional.of(template);
                } else if (Math.abs(onDemandTotalCost - naturalOnDemandTotalCost) < SMAUtils.EPSILON
                        && Math.abs(onDemandTotalCostWithPenalty - naturalOnDemandTotalCostWithPenalty) < SMAUtils.EPSILON
                        && (template.getOid() == currentTemplate.getOid())) {
                    // ondemand cost the same. penalty the same. current template breaks the tie.
                    naturalOptional = Optional.of(template);
                }
            }
            if (minCostProviderPerFamily.get(template.getFamily()) == null) {
                minCostProviderPerFamily.put(template.getFamily(), template);
            } else {
                float discountedTotalCost = getDiscountedTotalCost(costContext, template);
                float minCostProviderOnDemandTotalCost =
                        getOnDemandTotalCost(costContext, minCostProviderPerFamily.get(template.getFamily()));
                float minCostProviderDiscountedTotalCost =
                        getDiscountedTotalCost(costContext, minCostProviderPerFamily.get(template.getFamily()));
                float minCostProviderOnDemandTotalCostWithPenalty = minCostProviderOnDemandTotalCost
                        + minCostProviderPerFamily.get(template.getFamily()).getScalingPenalty();
                if (onDemandTotalCost - minCostProviderOnDemandTotalCost < (-1 * SMAUtils.EPSILON)) {
                    // ondemand cost breaks the tie
                    minCostProviderPerFamily.put(template.getFamily(), template);
                } else if (Math.abs(onDemandTotalCost - minCostProviderOnDemandTotalCost) < SMAUtils.EPSILON
                        && (discountedTotalCost - minCostProviderDiscountedTotalCost) < (-1 * SMAUtils.EPSILON)) {
                    // ondemand cost the same. discounted cost breaks the tie.
                    minCostProviderPerFamily.put(template.getFamily(), template);
                } else if (Math.abs(discountedTotalCost - minCostProviderDiscountedTotalCost) < SMAUtils.EPSILON
                        && Math.abs(onDemandTotalCost - minCostProviderOnDemandTotalCost) < SMAUtils.EPSILON
                        && (onDemandTotalCostWithPenalty - minCostProviderOnDemandTotalCostWithPenalty) < (-1 * SMAUtils.EPSILON)) {
                    // ondemand cost same. discounted cost the same. penality break the tie.
                    minCostProviderPerFamily.put(template.getFamily(), template);
                } else if (Math.abs(discountedTotalCost - minCostProviderDiscountedTotalCost) < SMAUtils.EPSILON
                        && Math.abs(onDemandTotalCost - minCostProviderOnDemandTotalCost) < SMAUtils.EPSILON
                        && Math.abs(onDemandTotalCostWithPenalty - minCostProviderOnDemandTotalCostWithPenalty) < SMAUtils.EPSILON
                        && (template.getOid() == currentTemplate.getOid())) {
                    // discounted cost the same. ondemand cost the same. penalty the same. current template breaks the tie.
                    minCostProviderPerFamily.put(template.getFamily(), template);
                }
            }
        }
        // If no minimum  is found, then use the current as the natural one
        smaVirtualMachineProvider.setNaturalTemplate(naturalOptional.orElse(currentTemplate));
        smaVirtualMachineProvider.setMinCostProviderPerFamily(minCostProviderPerFamily);
    }

    /**
     * Lookup the on-demand total cost for the business account.
     * @param costContext instance containing all the parameters for cost lookup.
     * @param smaTemplate the template for which the cost is computed.
     * @return on-demand total cost or Float.MAX_VALUE if not found.
     */
    public float getOnDemandTotalCost(CostContext costContext, SMATemplate smaTemplate) {
        Map<OSType, SMACost> costMap = smaTemplate.getOnDemandCosts().get(costContext.getBusinessAccount());
        SMACost cost = costMap != null ? costMap.get(costContext.getOsType()) : null;
        if (cost == null) {
            logger.debug("getOnDemandTotalCost: OID={} name={} has no on demand cost for {}",
                    smaTemplate.getOid(), smaTemplate.getName(), costContext);
            return Float.MAX_VALUE;
        }
        return getOsLicenseModelBasedCost(cost, costContext.getOsLicenseModel());
    }

    /**
     * Lookup the discounted total cost for the business account.
     *
     * @param costContext instance containing all the parameters for cost lookup.
     * @param smaTemplate the template for which the cost is computed.
     * @return discounted total cost or Float.MAX_VALUE if not found.
     */
    public float getDiscountedTotalCost(CostContext costContext, SMATemplate smaTemplate) {
        Map<OSType, SMACost> costMap = smaTemplate.getDiscountedCosts().get(costContext.getBusinessAccount());
        SMACost cost = costMap != null ? costMap.get(costContext.getOsType()) : null;
        if (cost == null) {
            logger.debug("getDiscountedTotalCost: OID={} name={} has no discounted cost for {}",
                    smaTemplate.getOid(), smaTemplate.getName(), costContext);
            return Float.MAX_VALUE;
        }
        return getOsLicenseModelBasedCost(cost, costContext.getOsLicenseModel());
    }

    /**
     * get the getOsLicenseModelBasedCost for the given SMACost.
     * @param cost SMACost of interest.
     * @param osLicenseModel the license model.
     * @return the cost based on license model.
     */
    private float getOsLicenseModelBasedCost(final SMACost cost,
            final LicenseModel osLicenseModel) {
        if (osLicenseModel == LicenseModel.LICENSE_INCLUDED) {
            return SMAUtils.round(cost.getTotal());
        } else {
            return SMAUtils.round(cost.getCompute());
        }
    }

    /**
     * compute the net cost based on discounted coupons and onDemandCost.
     * For AWS, the cost is only non-discounted portion times the onDemand cost.
     * For Azure, their may be a non zero discounted cost, which is applied to the discounted coupons.
     *
     * @param costContext instance containing all the parameters for cost lookup.
     * @param discountedCoupons discounted coupons
     * @param smaTemplate the template for which the cost is computed.
     * @return cost after applying discounted coupons.
     */
    public float getNetCost(CostContext costContext, float discountedCoupons, SMATemplate smaTemplate) {
        final float netCost;
        float coupons = smaTemplate.getCoupons();
        if (coupons < SMAUtils.BIG_EPSILON) {
            // If a template has 0 coupons, then it can't be discounted by a RI, and the on-demand
            // cost is returned.  E.g. Standard_A2m_v2.
            netCost = getOnDemandTotalCost(costContext, smaTemplate);
        } else if (discountedCoupons >= coupons) {
            netCost = getDiscountedTotalCost(costContext, smaTemplate);
        } else {
            float discountPercentage = discountedCoupons / coupons;
            netCost = (getDiscountedTotalCost(costContext, smaTemplate) * discountPercentage)
                    + (getOnDemandTotalCost(costContext, smaTemplate) * (1 - discountPercentage));
        }
        return SMAUtils.round(netCost);
    }

    /**
     * Compute the savings obtained by matching the virtual machine with reservedInstance.
     *
     * @param vm virtual machine of interest.
     * @param virtualMachineGroupMap  map from group name to virtualMachine Group
     * @param coupons remaining coupons available in the RI.
     * @param reservedInstance the reserved instance for which we compute the saving.
     * @return  the savings obtained by matching the virtual machine with reservedInstance.
     */
    public float computeSaving(SMAVirtualMachine vm,
            Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
            float coupons, SMAReservedInstance reservedInstance) {
        final List<SMAVirtualMachine> vmList;
        if (vm.getGroupSize() > 1) {
            vmList = virtualMachineGroupMap.get(vm.getGroupName()).getVirtualMachines();
        } else {
            vmList = Collections.singletonList(vm);
        }
        SMATemplate riTemplate = reservedInstance.getNormalizedTemplate();
        float netSavingvm = 0;
        for (SMAVirtualMachine member : vmList) {
            float onDemandCostvm =
                    getNetCost(vm.getCostContext(), 0, member.getNaturalTemplate());
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
            float afterMoveCostvm = reservedInstance.isIsf()
                    ? getNetCost(
                    vm.getCostContext(),
                    coupons / vm.getGroupSize(),
                    member.getMinCostProviderPerFamily(
                            riTemplate.getFamily()))
                    : getNetCost(vm.getCostContext(),
                            coupons / vm.getGroupSize(), riTemplate);

            netSavingvm += (onDemandCostvm - afterMoveCostvm);
        }
        return netSavingvm;

    }

    /**
     * compare the saving per coupon of two virtual machines when discounted by this RI.
     *
     * @param vm1 first VM
     * @param vm2 second VM
     * @param virtualMachineGroupMap  map from group name to virtualMachine Group
     * @param coupons remaining coupons available in the RI.
     * @param reservedInstance the reserved instance for which we are comparing the cost.
     * @return -1 if vm1 has more saving per coupon. 1 if vm2 has more saving per coupon. 0 otherwise.
     */
    public int compareCost(SMAVirtualMachine vm1, SMAVirtualMachine vm2,
            Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
            float coupons, SMAReservedInstance reservedInstance) {
        float netSavingVm1 = computeSaving(vm1, virtualMachineGroupMap, coupons, reservedInstance);
        float netSavingVm2 = computeSaving(vm2, virtualMachineGroupMap, coupons, reservedInstance);

        String riFamily = reservedInstance.getNormalizedTemplate().getFamily();
        float couponsVm1 = 0.0f;
        float couponsVm2 = 0.0f;
        if (reservedInstance.isIsf()) {
            couponsVm1 = Math.min(coupons, vm1.getMinCostProviderPerFamily(riFamily).getCoupons() * vm1.getGroupSize());
            couponsVm2 = Math.min(coupons, vm2.getMinCostProviderPerFamily(riFamily).getCoupons() * vm2.getGroupSize());
        } else {
            couponsVm1 = reservedInstance.getNormalizedTemplate().getCoupons() * vm1.getGroupSize();
            couponsVm2 = reservedInstance.getNormalizedTemplate().getCoupons() * vm2.getGroupSize();
        }

        float netSavingVm1PerCoupon = netSavingVm1 / couponsVm1;
        float netSavingVm2PerCoupon = netSavingVm2 / couponsVm2;

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

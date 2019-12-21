package com.vmturbo.market.cloudvmscaling.entities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.market.cloudvmscaling.analysis.SMAUtils;

/**
 * Stable Marriage Algorithm representation of a VM.
 */
public class SMAVirtualMachine {
    /**
     * Invariants
     **/
    /*
     * Unique identifier
     */
    private final long oid;
    /*
     * Name of VM, unique per context
     */
     final String name;
    /*
     * Name of group, unique per context
     */
    private final long groupOid;
    /*
     * BusinessAccount, subaccount of billing account
     */
    private final long businessAccount;
    /*
     * VM's current template.  Infer CSP, family and coupons
     */
    private final SMATemplate currentTemplate;
    /*
     * Current RI coverage as a percentage of coupons.  If 0, then no coverage.
     * Could be used to give this VM preference for RI coverage over VMs that did not have RI coverage
     */
    private final float currentRICoverage;
    /*
     * Cloud Zone
     */
    private final long zone;
    /*
     * The least cost provider for this VM in each family.
     * Set in updateNaturalTemplateAndMinCostProviderPerFamily()
     */
    private HashMap<String, SMATemplate> minCostProviderPerFamily;

    /**
     * Computed attributes
     */
    /*
     * The least cost provider for this VM.
     * Set in updateNaturalTemplateAndMinCostProviderPerFamily() also may be reset if in ASG to
     * the ASG leadres natural template
     */
    private SMATemplate naturalTemplate;
    /*
     * List of groupProviders (templates) that this VM could move to.
     * If the VM is in an ASG and not the leader, groupProviders is set to null
     */
    private List<SMATemplate> groupProviders;
    /*
     * The original set of Providers.
     */
    private final List<SMATemplate> providers;

    /*
     * The size of the group the VM belongs to.
     * if VM does not belong to any group groupSize = 1
     * if VM is not the leader of the group groupSize = 0
     */
    private int groupSize;

    /**
     *  Constructor for SMAVirtualMachine.
     *
     * @param oid the unique id of the virtual machine
     * @param name the display name of the
     * @param groupOid oid of the group the VM belongs to.
     * @param businessAccount the buisness account
     * @param currentTemplate the current template of the vm
     * @param providers the list of templates that the vm can fit in.
     * @param currentRICoverage the current RI converage of the VM
     * @param zone the zone to which the vm belongs to.
     */
    public SMAVirtualMachine(@Nonnull final long oid,
                             @Nonnull final String name,
                             final long groupOid,
                             final long businessAccount,
                             @Nonnull SMATemplate currentTemplate,
                             @Nonnull List<SMATemplate> providers,
                             final float currentRICoverage,
                             final long zone) {
        this.oid = Objects.requireNonNull(oid, "OID is null!");
        this.name = Objects.requireNonNull(name, "name is null!");
        this.groupOid = groupOid;
        this.currentTemplate = Objects.requireNonNull(currentTemplate, "currentTemplate is null!");
        this.businessAccount = businessAccount;
        this.currentRICoverage = currentRICoverage;
        this.groupProviders = Objects.requireNonNull(providers, "groupProviders is null!");
        this.providers = new ArrayList(providers);
        this.zone = zone;
        this.groupSize = 1;
        updateNaturalTemplateAndMinCostProviderPerFamily();
    }

    /**
     * Sets naturalTemplate: the natural (least cost) template.
     * Sets minCostProviderPerFamily: the least cost template on a per family basis
     */
    public void updateNaturalTemplateAndMinCostProviderPerFamily() {
        this.minCostProviderPerFamily = new HashMap<>();
        Optional<SMATemplate> minCostProvider = Optional.empty();
        float minCost = Float.MAX_VALUE;
        HashMap<String, Float> minCostPerFamily = new HashMap<>();
        Collections.sort(groupProviders, new SortTemplateByOID());
        for (SMATemplate template : groupProviders) {
            minCostPerFamily.put(template.getFamily(), Float.MAX_VALUE);
        }
        for (SMATemplate template : groupProviders) {
            if (template.getOnDemandCost().getTotal() - minCost < (-1.0 * SMAUtils.EPSILON)) {
                minCost = template.getOnDemandCost().getTotal();
                minCostProvider = Optional.of(template);
            } else {
                // Template cost is equal, then switch if the new template is the natural
                // template.
                if ((Math.abs(template.getOnDemandCost().getTotal() - minCost) < SMAUtils.EPSILON) &&
                        getCurrentTemplate().equals(template)) {
                        minCostProvider = Optional.of(template);
                }
            }
            if (template.getOnDemandCost().getTotal() - minCostPerFamily.get(template.getFamily()) < SMAUtils.EPSILON) {
                minCostPerFamily.put(template.getFamily(), template.getOnDemandCost().getTotal());
                minCostProviderPerFamily.put(template.getFamily(), template);
            }

        }
        // If no minimum  is found, then use the current as the natural one
        naturalTemplate = minCostProvider.orElse(getCurrentTemplate());
    }

    @Nonnull
    public long getOid() {
        return oid;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public List<SMATemplate> getProviders() {
        return providers;
    }

    @Nonnull
    public long getBusinessAccount() {
        return businessAccount;
    }

    @Nonnull
    public SMATemplate getCurrentTemplate() {
        return currentTemplate;
    }

    public long getGroupOid() {
        return groupOid;
    }

    public SMATemplate getNaturalTemplate() {
        return naturalTemplate;
    }

    public void setNaturalTemplate(SMATemplate template) {
        naturalTemplate = template;
    }

    public int getGroupSize() {
        return groupSize;
    }

    public void setGroupSize(final int groupSize) {
        this.groupSize = groupSize;
    }

    /**
     * return the provider with the least cost in the family. The smallest template.
     * @param family family of interest
     * @return template with the least cost in the family.
     */
    @Nullable
    public SMATemplate getMinCostProviderPerFamily(String family) {
        return minCostProviderPerFamily.get(family);
    }

    @Nonnull
    public List<SMATemplate> getGroupProviders() {
        return groupProviders;
    }

    public void setGroupProviders(final List<SMATemplate> groupProviders) {
        this.groupProviders = groupProviders;
    }

    /*
     * if 0 then no RI coverage
     */
    public float getCurrentRICoverage() {
        return currentRICoverage;
    }

    public long getZone() {
        return zone;
    }

    /**
     * converts SMAVirtualMachine to string.
     *
     * @return SMAVirtualMachine converted to string.
     */
    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("SMAVirtualMachine{")
                .append("OID='").append(oid)
                .append("name='").append(name)
                .append("', businessAccount='").append(businessAccount)
                .append("', currentTemplate=").append(currentTemplate)
                .append(", naturalTemplate=").append(naturalTemplate)
                .append(", groupProviders=").append(groupProviders.size())
                .append(", currentRICoverage=").append(currentRICoverage)
                .append(", zone='").append(zone).append("\'}");
        return buffer.toString();
    }

    /**
     * converts SMAVirtualMachine to string with subclasses only as string.
     * @return SMAVirtualMachine converted to string.
     */
    public String toStringShallow() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("SMAVirtualMachine{")
                 .append("OID='").append(oid)
                .append(", name='").append(name)
                .append("', businessAccount='").append(businessAccount)
                .append("', currentTemplate=").append(currentTemplate.getName())
                .append(", naturalTemplate=").append(naturalTemplate.getName())
                .append(", groupProviders=").append(groupProviders.size())
                .append(", currentRICoverage=").append(currentRICoverage)
                .append(", zone='").append(zone).append("\'}");
        return buffer.toString();
    }


    /**
     * checks if the virtual machine and reserved instance belong to the same zone.
     *
     * @param ri the reserved instance
     * @param virtualMachineGroupMap  the virtual Machine Group info
     * @return true if the vm and ri belong to the same zone or if the ri is region scoped
     */
    public boolean zoneCompatible(SMAReservedInstance ri,
                                  Map<Long, SMAVirtualMachineGroup> virtualMachineGroupMap) {
        /*
         * If the vm is the leader of ASG and the vms belong to different zones only a regional RI can
         * discount it.
         * All vms same zone -> both regional RI and zonal RI on same zone can discount it. isZonalDiscountable = true;
         * vms belong to different zone -> only regional RI can discount it. isZonalDiscountable = false;
         *
         */

        if (groupOid != SMAUtils.NO_GROUP_OID
                && !virtualMachineGroupMap.get(this.getGroupOid()).isZonalDiscountable()
                && !ri.isRegionScoped()) {
            return false;
        }
        if (ri.isRegionScoped()) {
            return true;
        } else {
            if (zone == ri.getZone()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Given two SMATemplate, compare by OID.
     */

    public static class SortTemplateByOID implements Comparator<SMATemplate> {
        @Override
        public int compare(SMATemplate template1, SMATemplate template2) {
            return (template1.getOid() - template2.getOid() > 0) ? 1 : -1;
        }
    }

}

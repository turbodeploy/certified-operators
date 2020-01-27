package com.vmturbo.market.cloudscaling.sma.entities;

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

import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;

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
    private final String name;
    /*
     * Name of  auto scaling group unique per context
     */
    private final String groupName;
    /*
     * BusinessAccount, subaccount of billing account
     */
    private final long businessAccount;
    /*
     * VM's current template.  Infer CSP, family and coupons
     */
    private SMATemplate currentTemplate;
    /*
     * The original set of Providers.
     */
    private List<SMATemplate> providers;
    /*
     * Current RI coverage as the number of coupons that are discounted; that is, covered by and RI.
     * If 0, then no coverage.
     * Could be used to give this VM preference for RI coverage over VMs that did not have RI coverage.
     * Used in Stability testing.
     * Not known at construction time, because need to process compute tiers to get number of coupons.
     */
    private float currentRICoverage;
    /**
     * Current RIKeyID.
     */
    private long currentRIKey;

    /*
     * Cloud Zone
     */
    private final long zone;
    /*
     * List of groupProviders (templates) that this VM could move to.
     * If the VM is in an ASG and not the leader, groupProviders is set to null
     */
    private List<SMATemplate> groupProviders;

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
     * @param groupName unique name of the group the VM belongs to.
     * @param businessAccount the buisness account
     * @param currentTemplate the current template of the vm
     * @param providers the list of templates that the vm can fit in.
     * @param currentRICoverage the current RI converage of the VM
     * @param zone the zone to which the vm belongs to.
     * @param currentRIKey ID of the current RI covering the VM.
     */
    public SMAVirtualMachine(final long oid,
                             @Nonnull final String name,
                             final String groupName,
                             final long businessAccount,
                             SMATemplate currentTemplate,
                             @Nonnull List<SMATemplate> providers,
                             final float currentRICoverage,
                             final long zone,
                             final long currentRIKey) {
        this.oid = oid;
        this.name = Objects.requireNonNull(name, "name is null!");
        this.groupName = groupName;
        this.currentTemplate = currentTemplate;
        this.businessAccount = businessAccount;
        this.currentRICoverage = currentRICoverage;
        this.groupProviders = Objects.requireNonNull(providers, "providers are null!");
        this.providers = new ArrayList(providers);
        this.zone = zone;
        this.groupSize = 1;
        this.currentRIKey = currentRIKey;
    }

    /**
     * Sets naturalTemplate: the natural (least cost) template.
     * Sets minCostProviderPerFamily: the least cost template on a per family basis.
     * Call only after templates have been processed fully.
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
            if (template.getOnDemandTotalCost(businessAccount) - minCost < (-1.0 * SMAUtils.EPSILON)) {
                minCost = template.getOnDemandTotalCost(businessAccount);
                minCostProvider = Optional.of(template);
            } else {
                // Template cost is equal, then switch if the new template is the natural
                // template.
                if ((Math.abs(template.getOnDemandTotalCost(businessAccount) - minCost) < SMAUtils.EPSILON) &&
                        getCurrentTemplate().equals(template)) {
                        minCostProvider = Optional.of(template);
                }
            }
            if (template.getOnDemandTotalCost(businessAccount) - minCostPerFamily.get(template.getFamily()) < SMAUtils.EPSILON) {
                minCostPerFamily.put(template.getFamily(), template.getOnDemandTotalCost(businessAccount));
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

    /**
     * When converting from market data structures, don't know providers until after templates are
     * processed.
     * @param providers list of providers as SMATemplates.
     */
    public void setProviders(final List<SMATemplate> providers) {
        this.providers = new ArrayList<>(providers);
        this.groupProviders = providers;
    }

    @Nonnull
    public long getBusinessAccount() {
        return businessAccount;
    }

    @Nonnull
    public SMATemplate getCurrentTemplate() {
        return currentTemplate;
    }

    public void setCurrentTemplate(SMATemplate template) {
        currentTemplate = template;
    }

    public String getGroupName() {
        return groupName;
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

    public void setCurrentRICoverage(float coverage) {
        currentRICoverage = coverage;
    }

    public long getCurrentRIKey() {
        return currentRIKey;
    }

    public void setCurrentRIKey(final long currentRIKey) {
        this.currentRIKey = currentRIKey;
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
                                  Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap) {
        /*
         * If the vm is the leader of ASG and the vms belong to different zones only a regional RI can
         * discount it.
         * All vms same zone -> both regional RI and zonal RI on same zone can discount it. isZonalDiscountable = true;
         * vms belong to different zone -> only regional RI can discount it. isZonalDiscountable = false;
         *
         */

        if (!groupName.equals(SMAUtils.NO_GROUP_ID)
                && !virtualMachineGroupMap.get(this.getGroupName()).isZonalDiscountable()
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

     @Override
     public int hashCode() {
         return Objects.hash(oid, name, businessAccount, zone);
     }

     @Override
     public boolean equals(final Object o) {
         if (this == o) {
             return true;
         }
         if (o == null || getClass() != o.getClass()) {
             return false;
         }
         final SMAVirtualMachine that = (SMAVirtualMachine)o;
         return oid == that.oid &&
             name.equals(that.name) &&
             businessAccount == that.businessAccount &&
             zone == that.zone;
     }
}

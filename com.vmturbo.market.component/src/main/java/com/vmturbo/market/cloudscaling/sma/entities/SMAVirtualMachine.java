package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

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
     * BusinessAccount, subaccount of billing account.  Needed to compute template cost.
     */
    private final long businessAccountId;
    /*
     * OSType: needed to compute template cost
     */
    private final OSType osType;
    /*
     * Cloud Zone
     */
    private final long zoneId;

    /*
    * the region id of the VM
     */
    private final long regionId;

    /*
     * the account pricing data oid. This uniquiely detemines the price table to look for.
     */
    private final long accountPricingDataOid;


    /**
     * operating system license model, e.g. license included, bring your own license.
     */
    private final LicenseModel operatingSystemLicenseModel;

    /*
     * Not invariants.  After SMAReservedInstance and SMATemplates are discovered.
     */
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
    /*
     * Current Reserved Instance
     */
    @Nullable
    private SMAReservedInstance currentRI;

    /*
     * Computed attributes.
     */
    /*
     * List of groupProviders (templates) that this VM could move to.
     * If the VM is in an ASG and not the leader, groupProviders is empty
     */
    private List<SMATemplate> groupProviders;

    /*
     * The least cost provider for this VM in each family.
     * Set in updateNaturalTemplateAndMinCostProviderPerFamily()
     */
    private HashMap<String, SMATemplate> minCostProviderPerFamily;

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

    private boolean scaleUp;

    /**
     *  Constructor for SMAVirtualMachine.
     *
     * @param oid the unique id of the virtual machine
     * @param name the display name of the
     * @param groupName unique name of the group the VM belongs to.
     * @param businessAccountId the business account ID
     * @param currentTemplate the current template of the VM
     * @param providers the list of templates that the VM can fit in.
     * @param currentRICoverage the current RI converage of the VM
     * @param zoneId the zone ID to which the VM belongs to.
     * @param currentRI current RI covering the VM.
     * @param osType  OS.
     * @param operatingSystemLicenseModel os license model.
     * @param scaleUp true if the vm has a scaleup action
     */
    public SMAVirtualMachine(final long oid,
                             @Nonnull final String name,
                             final String groupName,
                             final long businessAccountId,
                             SMATemplate currentTemplate,
                             @Nonnull List<SMATemplate> providers,
                             final float currentRICoverage,
                             final long zoneId,
                             final SMAReservedInstance currentRI,
                             final OSType osType,
                             @Nullable final LicenseModel operatingSystemLicenseModel,
                             boolean scaleUp,
                             @Nonnull List<SMATemplate> groupProviders,
                             SMATemplate naturalTemplate,
                             HashMap<String, SMATemplate> minCostProviderPerFamily,
                             final long regionId,
                             final long accountPricingDataOid) {
        this.oid = oid;
        this.name = Objects.requireNonNull(name, "name is null!");
        this.groupName = groupName;
        this.currentTemplate = currentTemplate;
        this.businessAccountId = businessAccountId;
        this.currentRICoverage = currentRICoverage;
        this.zoneId = zoneId;
        this.groupSize = 1;
        this.currentRI = currentRI;
        this.osType = osType;
        // operatingSystemLicenseModel argument will not be null in production as the the
        // protobuf field has a default defined. The null check below is for test inputs which
        // may not have operatingSystemLicenseModel defined.
        this.operatingSystemLicenseModel = operatingSystemLicenseModel == null
            ? LicenseModel.LICENSE_INCLUDED : operatingSystemLicenseModel;
        this.providers = providers;
        this.groupProviders = groupProviders;
        this.minCostProviderPerFamily = minCostProviderPerFamily;
        this.naturalTemplate = naturalTemplate;
        this.scaleUp = scaleUp;
        this.regionId = regionId;
        this.accountPricingDataOid = accountPricingDataOid;
    }

    public void setVirtualMachineProviderInfo(SMAVirtualMachineProvider smaVirtualMachineProvider) {
        this.providers = smaVirtualMachineProvider.getProviders();
        this.groupProviders = smaVirtualMachineProvider.getGroupProviders();
        this.naturalTemplate = smaVirtualMachineProvider.getNaturalTemplate();
        this.minCostProviderPerFamily = smaVirtualMachineProvider.getMinCostProviderPerFamily();
    }

    public void setVirtualMachineProviderInfoWithoutProviders(SMAVirtualMachineProvider smaVirtualMachineProvider) {
        this.groupProviders = smaVirtualMachineProvider.getGroupProviders();
        this.naturalTemplate = smaVirtualMachineProvider.getNaturalTemplate();
        this.minCostProviderPerFamily = smaVirtualMachineProvider.getMinCostProviderPerFamily();
    }

    public boolean isScaleUp() {
        return scaleUp;
    }

    public void setScaleUp(final boolean scaleUp) {
        this.scaleUp = scaleUp;
    }

    public boolean isEmptyProviderList() {
        return (providers == null || providers.isEmpty());
    }

    public long getAccountPricingDataOid() {
        return accountPricingDataOid;
    }

    public long getRegionId() {
        return regionId;
    }

    public void setMinCostProviderPerFamily(final HashMap<String, SMATemplate> minCostProviderPerFamily) {
        this.minCostProviderPerFamily = minCostProviderPerFamily;
    }

    public HashMap<String, SMATemplate> getMinCostProviderPerFamily() {
        return minCostProviderPerFamily;
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
    public long getBusinessAccountId() {
        return businessAccountId;
    }

    @Nonnull
    public OSType getOsType() {
        return osType;
    }

    @Nonnull
    public SMATemplate getCurrentTemplate() {
        return currentTemplate;
    }

    @Nonnull
    public LicenseModel getOsLicenseModel() {
        return operatingSystemLicenseModel;
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

    public List<SMATemplate> getGroupProviders() {
        return groupProviders;
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

    public SMAReservedInstance getCurrentRI() {
        return currentRI;
    }

    public void setCurrentRI(final SMAReservedInstance currentRI) {
        this.currentRI = currentRI;
    }

    public long getZoneId() {
        return zoneId;
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
                .append(", name='").append(name)
                .append("', businessAccount='").append(businessAccountId)
                .append(", groupProviders=").append(groupProviders.size())
                .append(", currentRICoverage=").append(currentRICoverage)
                .append(", zone='").append(zoneId)
                .append("', currentTemplate=").append(currentTemplate)
                .append(", naturalTemplate=").append(naturalTemplate)
            .append("\'}");
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
            .append("', businessAccount='").append(businessAccountId)
            .append("', OS='").append(osType.name())
            .append(", currentRICoverage=").append(currentRICoverage)
            .append(", zone='").append(zoneId)
            .append("', currentTemplate=").append(currentTemplate.getName())
            .append(", naturalTemplate=").append(naturalTemplate.getName())
            .append(", groupProviders=").append(groupProviders.size())
            .append("\'}");
        return buffer.toString();
    }

    /**
     * checks if the virtual machine and reserved instance belong to the same zone.
     *
     * @param ri the reserved instance
     * @param virtualMachineGroupMap  the virtual Machine Group info
     * @return true if the VM and RI belong to the same zone or if the RI is region scoped
     */
    public boolean zoneCompatible(SMAReservedInstance ri,
                                  Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap) {
        /*
         * If the VM is the leader of ASG and the VMs belong to different zones only a regional RI can
         * discount it.
         * All VMs same zone -> both regional RI and zonal RI on same zone can discount it. isZonalDiscountable = true;
         * VMs belong to different zone -> only regional RI can discount it. isZonalDiscountable = false;
         *
         */

        if ((getGroupSize() > 1)
                && !virtualMachineGroupMap.get(this.getGroupName()).isZonalDiscountable()
                && !ri.isRegionScoped()) {
            return false;
        }
        if (ri.isRegionScoped()) {
            return true;
        } else {
            if (zoneId == ri.getZoneId()) {
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

    /**
     * Check if an RI may be applied to this VM.
     *
     * @param ri an RI
     * @return true if an RI may be applied to this VM
     */
    public boolean mayBeCoveredByRI(SMAReservedInstance ri) {
        return ri.isShared() || ri.getApplicableBusinessAccounts().contains(getBusinessAccountId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(oid, name, businessAccountId, zoneId);
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
                businessAccountId == that.businessAccountId &&
                zoneId == that.zoneId;
    }

    // Compression for diags related code

    private long currentTemplateOid;
    private List<Long> providersOid = new ArrayList();
    private long currentRIOID;

    public long getCurrentRIOID() {
        return currentRIOID;
    }

    public void setCurrentRIOID(final long currentRIOID) {
        this.currentRIOID = currentRIOID;
    }

    public void setCurrentTemplateOid(final long currentTemplateOid) {
        this.currentTemplateOid = currentTemplateOid;
    }

    public long getCurrentTemplateOid() {
        return currentTemplateOid;
    }

    public List<Long> getProvidersOid() {
        return providersOid;
    }
}

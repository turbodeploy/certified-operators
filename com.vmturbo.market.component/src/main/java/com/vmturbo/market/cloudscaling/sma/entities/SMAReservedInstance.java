package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.cloud.common.commitment.CommitmentAmountCalculator;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;

/**
 * Stable marriage algorithm representation of an RI.
 */

public class SMAReservedInstance {

    /*
     * identifier of RIBought. Unique identifier.
     */
    private final long oid;
    /*
     * identifier of riKey. it is generated from the ReservedInstanceSpec and
     * ReservedInstanceBought DTOs to identify the attributes
     * that allow RIs to be aggregated without violating scoping rules.
     */
    private final long riKeyOid;
    /*
     * name of RI
     */
    private final String name;

    /*
     * BusinessAccount, subaccount of the billing family.
     */
    private final long businessAccountId;

    /**
     * Scoped BA oids. RI scoped account may be different from purchase account.
     */
    private final Set<Long> applicableBusinessAccounts;

    /*
     * Template, used to infer CSP, family and coupons.
     */

    private SMATemplate template;

    /*
     * The count of coupons this RI represents.  Count is a float, and not an int, to handle VMs
     * that are covered by this RI, but isEligibleForScale is false.  For example, there is an
     * RI that is t2.xlarge (24 coupons) and ISF, and it covers a VM that is t2.large (12 coupons);
     * however, the VM has isEligibleForScale false.  Therefore, the RI only has 12 coupons available,
     * because the VM, which is not eligible for scale is using 12 coupons.  Therefore, the RIs
     * count is 0.5.
     */
    private final float count;

    /*
     * Reserved Instance is InstanceSizeFlexible or not based on context.
     */
    private final boolean isf;

    /*
     * Scope of RI: only applicable to Azure.  If true applies to Enterprise account, otherwise
     * only to the RI's business account.
     */
    private final boolean shared;

    /*
     * Reserved Instance is platform flexible or not based on context.
     */
    private final boolean platformFlexible;

    /*
     * Additional instance variables.
     */
    // the least cost template in the family for ISF RI's
    private SMATemplate normalizedTemplate;

    /**
     * The list of vms discountable by this RI. This is sorted at the beginning.
     */
    private Deque<SMAVirtualMachine> sortedVirtualMachineList;

    // the set of vms skipped by the RI. The ones which are not discounted.
    // we maintain this because when the RI gains coupons we have to add this back to the list.
    private Deque<SMAVirtualMachine> skippedVMs;

    /*
     * Availability zone.  If zone == NO_ZONE then regional RI.
     */
    private final long zoneId;

    // map to keep track of RI Coverage for each group.
    // Saved to avoid recomputing everytime.
    // ASG Group Name x Pair<couponsCoveredOfASG, totalCouponsOfASG>
    private HashMap<String, Pair<Float, Float>> riCoveragePerGroup;

    // last discounted VM
    private SMAVirtualMachine lastDiscountedVM;

    private CloudCommitmentAmount commitmentAmount;

    /**
     * SMA Reserved Instance.
     *
     * @param oid              oid of RI bought.
     * @param riKeyOid         generated keyid for ReservedInstanceKey
     * @param name             name of RI
     * @param businessAccount  business account
     * @param applicableBusinessAccounts scoped BAs
     * @param template         compute tier
     * @param zone             availability zone
     * @param count            number of RIs
     * @param isf              true if RI is instance size flexible
     * @param shared           true if RI is scoped to AWS billing family or Azure EA.
     *                         Otherwise, scoped to business account (only Azure)
     * @param platformFlexible true if RI is instance size flexible
     * @param commitmentAmount the commitment amount
     */
    public SMAReservedInstance(final long oid,
                               final long riKeyOid,
                               @Nonnull final String name,
                               final long businessAccount,
                               @Nonnull Set<Long> applicableBusinessAccounts,
                               @Nonnull final SMATemplate template,
                               final long zone,
                               final float count,
                               final boolean isf,
                               final boolean shared,
                               final boolean platformFlexible,
                               @Nonnull final CloudCommitmentAmount commitmentAmount) {
        this.oid = oid;
        this.riKeyOid = riKeyOid;
        this.name = Objects.requireNonNull(name, "name is null!");
        this.businessAccountId = businessAccount;
        this.applicableBusinessAccounts = applicableBusinessAccounts;
        this.template = Objects.requireNonNull(template, "template is null!");
        this.normalizedTemplate = template;
        this.zoneId = zone;
        this.count = count;
        this.isf = isf;
        this.shared = shared;
        this.platformFlexible = platformFlexible;
        this.sortedVirtualMachineList = new LinkedList<>();
        lastDiscountedVM = null;
        riCoveragePerGroup = new HashMap<>();
        skippedVMs = new LinkedList<>();
        this.commitmentAmount = commitmentAmount;
    }

    /**
     * Create a shallow copy of {@link SMAReservedInstance}.
     *
     * @param ri {@link SMAReservedInstance} instance to copy
     * @return shallow copy of provided  {@link SMAReservedInstance}
     */
    public static SMAReservedInstance copyFrom(SMAReservedInstance ri) {
        return new SMAReservedInstance(
                ri.getOid(),
                ri.getRiKeyOid(),
                ri.getName(),
                ri.getBusinessAccountId(),
                ri.getApplicableBusinessAccounts(),
                ri.getTemplate(),
                ri.getZoneId(),
                ri.getCount(),
                ri.isIsf(),
                ri.isShared(),
                ri.isPlatformFlexible(),
                ri.getCommitmentAmount());
    }

    /*
     * Getter and setters for constructor attributes.
     */
    public long getOid() {
        return oid;
    }

    public long getRiKeyOid() {
        return riKeyOid;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public long getBusinessAccountId() {
        return businessAccountId;
    }

    @Nonnull
    public SMATemplate getTemplate() {
        return template;
    }

    @Nonnull
    public SMATemplate getNormalizedTemplate() {
        return normalizedTemplate;
    }

    public void setNormalizedTemplate(final SMATemplate normalizedTemplate) {
        this.normalizedTemplate = normalizedTemplate;
    }

    public long getZoneId() {
        return zoneId;
    }

    public float getCount() {
        return count;
    }

    public boolean isIsf() {
        return isf;
    }

    public boolean isShared() {
        return shared;
    }

    public boolean isPlatformFlexible() {
        return platformFlexible;
    }

    /**
     * determines if the RI is regional or zonal.
     * @return returns true if the RI is regional. false if zonal.
     */
    public boolean isRegionScoped() {
        if (zoneId == SMAUtils.NO_ZONE) {
            return true;
        }
        return false;
    }

    public SMAVirtualMachine getLastDiscountedVM() {
        return lastDiscountedVM;
    }

    public void setLastDiscountedVM(final SMAVirtualMachine lastDiscountedVM) {
        this.lastDiscountedVM = lastDiscountedVM;
    }


    /**
     * Normalize the RI to the cheapest template in the family for ISF RIs.
     *
     * @param newTemplate new normalized template.
     */
    public void normalizeTemplate(SMATemplate newTemplate) {
        if (isIsf()) {
            setNormalizedTemplate(newTemplate);
        }
    }

    /**
     * Add vm to the skippedVms list.
     * @param vm vm to be skipped.
     */
    public void addToSkippedVMs(SMAVirtualMachine vm) {
        skippedVMs.addFirst(vm);
    }

    /**
     * restore all the skipped vms back to CouponToBestVM. And then clear the skippedVMsWIthIndex.
     */
    public void restoreSkippedVMs() {
        while (!skippedVMs.isEmpty()) {
            SMAVirtualMachine vm = skippedVMs.pop();
            addVMToDiscountableVMs(vm, true);
        }
    }

    /**
     * Checks if virtualMachineList map is empty. This signifies that the RI has proposed to every
     * possible VM.
     *
     * @return true if virtualMachineList is empty.
     */
    public boolean isDiscountableVMsEmpty() {
        return sortedVirtualMachineList.isEmpty();
    }

    /**
     * Add  vm to virtualMachineList. For retry we have to add the VM to the first in the list to
     * maintain the sorted order.
     *
     * @param vm      the VM to be added.
     * @param addFirst should the VM be added to end of beginning to the list.
     */
    public void addVMToDiscountableVMs(SMAVirtualMachine vm, boolean addFirst) {
        if (addFirst) {
            sortedVirtualMachineList.addFirst(vm);
        } else {
            sortedVirtualMachineList.addLast(vm);
        }
    }


    /**
     * Remove the 0th entry from the virtualMachineList.
     *
     */
    public void removeVMFromDiscountableVMs() {
        if (!sortedVirtualMachineList.isEmpty()) {
            sortedVirtualMachineList.removeFirst();
        }
    }

    /**
     * Find the best VM that can be discounted by this RI.
     *
     * @return the most preferred vm for the RI that the RI has not yet proposed
     */
    public SMAVirtualMachine findBestDiscountableVM() {
        if(sortedVirtualMachineList.isEmpty()) {
            return null;
        } else {
            return sortedVirtualMachineList.getFirst();
        }
    }



    /**
     * compute the coupons the vm group is currently discounted by this RI.
     *
     * @param virtualMachineGroup the vm group to be checked
     */
    public void updateRICoveragePerGroup(SMAVirtualMachineGroup virtualMachineGroup,
            SMACloudCostCalculator smaCloudCostCalculator) {
        Float coverage = 0f;
        Float total = 0f;
        for (SMAVirtualMachine member : virtualMachineGroup.getVirtualMachines()) {
            if (isVMDiscountedByThisRI(member)) {
                float memberCoverage = smaCloudCostCalculator.covertCommitmentToFloat(member,
                        member.getCurrentTemplate(), this.oid, member.getCurrentRICoverage());
                coverage = coverage + memberCoverage;
            }
            float totalMemberCoverage = smaCloudCostCalculator.covertCommitmentToFloat(member,
                    member.getCurrentTemplate(), this.oid,
                    member.getCurrentTemplate().getCommitmentAmount());
            total = total + totalMemberCoverage;
        }
        Pair<Float, Float> pair = new Pair<>(coverage, total);
        riCoveragePerGroup.put(virtualMachineGroup.getName(), pair);
    }

    /**
     * coupons vm  vmgroup is currently discounted by this RI.
     *
     * @param vm the vm to be checked
     * @return discounted coupons.
     */
    public float getRICoverage(SMAVirtualMachine vm, SMACloudCostCalculator smaCloudCostCalculator) {
        float riCoverage = 0;
        if (vm.getGroupSize() > 1) {
            riCoverage = (riCoveragePerGroup.get(vm.getGroupName()).first /
                     riCoveragePerGroup.get(vm.getGroupName()).second);
        } else {
            if (isVMDiscountedByThisRI(vm)) {
                float vmCoverage = smaCloudCostCalculator.covertCommitmentToFloat(vm,
                        vm.getCurrentTemplate(),this.getOid(),vm.getCurrentRICoverage());
                float totalCoverage = smaCloudCostCalculator.covertCommitmentToFloat(vm,
                        vm.getCurrentTemplate(),this.getOid(),vm.getCurrentTemplate().getCommitmentAmount());
                riCoverage = vmCoverage / totalCoverage;
            } else {
                return 0;
            }
        }
        return SMAUtils.round(riCoverage);
    }

    /**
     * checks if the vm is currently discounted by this RI.
     *
     * @param vm the vm to be checked
     * @return true if the vm is currently discounted.
     */

    public boolean isVMDiscountedByThisRI(SMAVirtualMachine vm) {
        return (vm.getCurrentRI() != null &&
            vm.getCurrentRI().getRiKeyOid() == getRiKeyOid()) &&
                CommitmentAmountCalculator.isPositive(vm.getCurrentRICoverage(), SMAUtils.EPSILON);
    }

    /**
     * Get list of accounts that RI can be applied to.
     *
     * @return List of accounts that RI can be applied to
     */
    public Set<Long> getApplicableBusinessAccounts() {
        return applicableBusinessAccounts;
    }

    public CloudCommitmentAmount getCommitmentAmount() {
        return commitmentAmount;
    }

    public void setCommitmentAmount(@Nonnull final CloudCommitmentAmount commitmentAmount) {
        this.commitmentAmount = commitmentAmount;
    }

    /**
     * Determine if two RIs are equivalent.  They are equivalent if they have same oid.
     *
     * @return hash code based on oid
     */
    @Override
    public int hashCode() {
        return Objects.hash(oid);
    }

    /*
     * RI's that have same businessAccount,normalizedTemplate and zone should be combined. This allows
     * multiple RI's to discount a single VM in case of ISF. Multiple RI to discount multiple VMs in case
     * of ASG.
     */
    /**
     * Determine if two RIs are equivalent.  They are equivalent if they have same oid.
     * @param obj the other RI
     * @return true if the RI's are equivalent.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SMAReservedInstance that = (SMAReservedInstance)obj;
        return oid == that.oid;
    }

    @Override
    public String toString() {
        return "SMAReservedInstance{" +
                "OID='" + oid + "'" +
                ", keyOID='" + riKeyOid + "'" +
                ", name='" + name + "'" +
                ", businessAccount='" + businessAccountId + "'" +
                ", zone='" + zoneId + "'" +
                ", isf=" + isf +
                ", platformFlexible=" + platformFlexible +
                ", shared=" + shared +
                ", normalizedTemplate=" + normalizedTemplate +
                ", commitmentAmount=" + commitmentAmount +
                '}';
    }

    // Compression for diags related code
    private long templateOid;

    public void setTemplate(final SMATemplate template) {
        this.template = template;
    }

    public long getTemplateOid() {
        return templateOid;
    }

    public void setTemplateOid(final long templateOid) {
        this.templateOid = templateOid;
    }
}
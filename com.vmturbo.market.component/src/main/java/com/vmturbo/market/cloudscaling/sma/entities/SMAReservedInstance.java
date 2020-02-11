package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;

/**
 * Stable marriage algorithm representation of an RI.
 */

public class SMAReservedInstance {
    /*
     * Set by the constructor
     */
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
    /*
     * Template, used to infer CSP, family and coupons.
     */

    private final SMATemplate template;


    private final int count;

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

    /*
     * For Instance Size Flexible and Auto Scaling Groups where we can do partial coverage we keep
     * track of separate list for each coupons range. We do this to make sure that we do
     * full coverage before we do the partial coverage. A partial coverage might use up lot of
     * coupons for very little saving because we still have to pay for uncovered portion. So
     * even if the vm is preferred skip it if it cannot be fully covered.
     */
    /*  We therefore compute coupons to list of pairs..
        the pair is the vm and the actual index in the RI preference list.
        assume the RI can discount 5 vms (vma vmb vmc vmd vme) in this order and they
        need 2 4 1 2 1 coupons respectively. Then couponToBestVM will look like this.
           1 -> [(vmc,2),(vme,5)]
           2 -> [(vma,0),(vmd,4)]
           4 -> [(vmb,1)]

        This is done so that we can know the preference of RI within a coupon family.
        Also we can get the most preferred vm which needs < x coupons efficiently.
     */
    private HashMap<Integer, List<Pair<SMAVirtualMachine, Integer>>> couponToBestVM;

    /*
     * list of VMs partitioned by business account id, to handle Azure single scoping.
     */
    private HashSet<SMAVirtualMachine> discountableVMs;

    /*
     * Availability zone.  If zone == NO_ZONE then regional RI.
     */
    private final long zoneId;

    /*
     * Total count is the count after we normalize the RI and combine the RIs.
     */
    private float normalizedCount;

    // map to keep track of RI Coverage for each group.
    // Saved to avoid recomputing everytime.
    // ASG Group Name x Pair<couponsCoveredOfASG, totalCouponsOfASG>
    private HashMap<String, Pair<Float, Float>> riCoveragePerGroup;

    // last discounted VM
    private SMAVirtualMachine lastDiscountedVM;

    /**
     * SMA Reserved Instance.
     *
     * @param oid              oid of RI bought.
     * @param riKeyOid         generated keyid for ReservedInstanceKey
     * @param name             name of RI
     * @param businessAccount  business account
     * @param template         compute tier
     * @param zone             availability zone
     * @param count            number of RIs
     * @param isf              true if RI is instance size flexible
     * @param shared           true if RI is scoped to AWS billing family or Azure EA.
     *                         Otherwise, scoped to business account (only Azure)
     * @param platformFlexible true if RI is instance size flexible
     */
    public SMAReservedInstance(final long oid,
                               final long riKeyOid,
                               @Nonnull final String name,
                               final long businessAccount,
                               @Nonnull final SMATemplate template,
                               final long zone,
                               final int count,
                               final boolean isf,
                               final boolean shared,
                               final boolean platformFlexible) {
        this.oid = oid;
        this.riKeyOid = riKeyOid;
        this.name = Objects.requireNonNull(name, "name is null!");
        this.businessAccountId = businessAccount;
        this.template = Objects.requireNonNull(template, "template is null!");
        this.normalizedTemplate = template;
        this.zoneId = zone;
        this.normalizedCount = count;
        this.count = count;
        this.isf = isf;
        this.shared = shared;
        this.platformFlexible = platformFlexible;
        couponToBestVM = new HashMap<>();
        lastDiscountedVM = null;
        riCoveragePerGroup = new HashMap<>();
        discountableVMs = new HashSet<>();
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

    public float getNormalizedCount() {
        return normalizedCount;
    }

    public int getCount() {
        return count;
    }

    public void setNormalizedCount(final float normalizedCount) {
        this.normalizedCount = normalizedCount;
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

    public HashSet<SMAVirtualMachine> getDiscountableVMs() {
        return discountableVMs;
    }



    /**
     * Normalize the RI to the cheapest template in the family for ISF RIs.
     *
     * @param newTemplate new normalized template.
     */
    public void normalizeTemplate(SMATemplate newTemplate) {
        if (isIsf()) {
            SMATemplate oldTemplate = getNormalizedTemplate();
            float countMultiplier = (float)oldTemplate.getCoupons() / (float)newTemplate.getCoupons();
            setNormalizedCount(getCount() * countMultiplier);
            setNormalizedTemplate(newTemplate);
        }
    }

    /**
     * Checks if couponToBestVM map is empty. This signifies that the RI has proposed to every
     * possible VM.
     *
     * @return true if couponToBestVM is empty.
     */
    public boolean isCouponToBestVMEmpty() {
        return couponToBestVM.keySet().isEmpty();
    }

    /**
     * Add  the pair (VM, vmIndex) to couponToBestVM.get(coupon).
     * Side Effect: updates discountableVMs.
     *
     * @param coupon  the key to search in couponToBestVM
     * @param vmIndex the index of VM in RI's preference list.
     * @param vm      the VM to be added.
     */
    public void addVMToCouponToBestVM(int coupon, Integer vmIndex, SMAVirtualMachine vm) {
        if (couponToBestVM.get(coupon) == null) {
            List<Pair<SMAVirtualMachine, Integer>> vmList =
                    new ArrayList(Arrays.asList(new Pair(vm, vmIndex)));
            couponToBestVM.put(coupon, vmList);
        } else {
            couponToBestVM.get(coupon).add(new Pair(vm, vmIndex));
        }
        discountableVMs.add(vm);
    }

    public HashMap<Integer, List<Pair<SMAVirtualMachine, Integer>>> getCouponToBestVM() {
        return couponToBestVM;
    }

    /**
     * remove the 0th entry from couponToBestVM.get(coupon).
     * Side Effect: updates discountableVMs.
     *
     * @param coupon the key to search in couponToBestVM
     */
    public void removeVMFromCouponToBestVM(int coupon) {
        List<Pair<SMAVirtualMachine, Integer>> vmList = couponToBestVM.get(coupon);
        if (vmList != null) {
            if (vmList.isEmpty()) {
                couponToBestVM.remove(coupon);
            } else {
                SMAVirtualMachine vm = vmList.get(0).first;
                discountableVMs.remove(vm);
                vmList.remove(0);
                if (vmList.isEmpty()) {
                    couponToBestVM.remove(coupon);
                }
            }
        }
    }

    /**
     * look at the 0th entry of all the lists in couponToBestVM whose key is <= coupon. return the vm whose
     * corresponding index is the least. If includePartialCoverage is true look for all keys in couponToBestVM
     * ignoring coupon value.
     *
     * @param coupon                 the max key value to search in couponToBestVM
     * @param includePartialCoverage ignore coupon parameter if true
     * @return the most preferred vm for the RI that the RI has not yet proposed and doesnt require more
     * coupons than what the RI has. In other words vm that can be fully covered by RI.
     */
    public SMAVirtualMachine findBestVMIndexFromCouponToBestVM(int coupon, boolean includePartialCoverage) {
        Set<Integer> possibleCoupons = couponToBestVM.keySet();
        if (!includePartialCoverage) {
            possibleCoupons = possibleCoupons.stream()
                    .filter(a -> a <= coupon).collect(Collectors.toSet());
        }
        SMAVirtualMachine bestVM = null;
        Integer bestIndex = Integer.MAX_VALUE;
        for (Integer c : possibleCoupons) {
            List<Pair<SMAVirtualMachine, Integer>> vmList = couponToBestVM.get(c);
            if (vmList != null && !vmList.isEmpty() && vmList.get(0).second < bestIndex) {
                bestIndex = vmList.get(0).second;
                bestVM = vmList.get(0).first;
            }
        }
        return bestVM;
    }

    /**
     * compute the coupons the vm group is currently discounted by this RI.
     *
     * @param virtualMachineGroup the vm group to be checked
     */
    public void updateRICoveragePerGroup(SMAVirtualMachineGroup virtualMachineGroup) {
        float coverage = 0;
        float total = 0;
        for (SMAVirtualMachine member : virtualMachineGroup.getVirtualMachines()) {
            if (isVMDiscountedByThisRI(member)) {
                coverage = coverage + member.getCurrentRICoverage();
            }
            total = total + member.getCurrentTemplate().getCoupons();
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
    public float getRICoverage(SMAVirtualMachine vm) {
        float riCoverage = 0;
        if (vm.getGroupSize() > 1) {
            riCoverage = riCoveragePerGroup.get(vm.getGroupName()).first
                    / riCoveragePerGroup.get(vm.getGroupName()).second;
        } else {
            if (isVMDiscountedByThisRI(vm)) {
                riCoverage = vm.getCurrentRICoverage()
                        / vm.getCurrentTemplate().getCoupons();
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
        float riCoverage = vm.getCurrentRICoverage();
        return (riCoverage > SMAUtils.EPSILON && vm.getCurrentRIKey() == getRiKeyOid());
    }

    /**
     * Determine if two RIs are equivalent.  They are equivalent if they have same oid.
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
                ", normalizedCount=" + normalizedCount +
                ", isf=" + isf +
                ", platformFlexible=" + platformFlexible +
                ", shared=" + shared +
                ", normalizedTemplate=" + normalizedTemplate.toStringWithOutCost() +
                '}';
    }
}
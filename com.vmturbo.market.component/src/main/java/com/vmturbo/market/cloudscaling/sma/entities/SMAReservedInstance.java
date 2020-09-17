package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
        Also note that the list is maintained sorted based on the index (the actual
        index in the RI preference list).
     */
    private HashMap<Integer, Deque<Pair<SMAVirtualMachine, Integer>>> couponToBestVM;

    /*
     * list of VMs partitioned by business account id, to handle Azure single scoping.
     */
    private HashSet<SMAVirtualMachine> discountableVMs;

    // the set of vms skipped by the RI. The ones which are not discounted.
    // we maintain this because when the RI gains coupons we have to add this back to the list.
    // the second entry in the list is the index of the vm in the ri's sorted discountable vm list.
    private Deque<Pair<SMAVirtualMachine, Integer>> skippedVMsWIthIndex;

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
     * @param applicableBusinessAccounts scoped BAs
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
                               @Nonnull Set<Long> applicableBusinessAccounts,
                               @Nonnull final SMATemplate template,
                               final long zone,
                               final float count,
                               final boolean isf,
                               final boolean shared,
                               final boolean platformFlexible) {
        this.oid = oid;
        this.riKeyOid = riKeyOid;
        this.name = Objects.requireNonNull(name, "name is null!");
        this.businessAccountId = businessAccount;
        this.applicableBusinessAccounts = applicableBusinessAccounts;
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
        skippedVMsWIthIndex = new LinkedList<>();
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
                ri.isPlatformFlexible());
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

    public float getCount() {
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
     * Add vm to the skippedVms list.
     * @param vm vm to be skipped.
     * @param index the actual index in the RI preference list
     */
    public void addToSkippedVMsWIthIndex(SMAVirtualMachine vm, int index) {
        skippedVMsWIthIndex.addFirst(new Pair(vm, index));
    }

    /**
     * restore all the skipped vms back to CouponToBestVM. And then clear the skippedVMsWIthIndex.
     */
    public void restoreSkippedVMs() {
        while (!skippedVMsWIthIndex.isEmpty()) {
            Pair<SMAVirtualMachine, Integer> entry = skippedVMsWIthIndex.pop();
            addVMToCouponToBestVM(entry.second, entry.first, true);
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
     * @param vmIndex the index of VM in RI's preference list.
     * @param vm      the VM to be added.
     * @param addFirst should the VM be added to end of beginning to the list.
     */
    public void addVMToCouponToBestVM(Integer vmIndex, SMAVirtualMachine vm, boolean addFirst) {
        int coupon = getTemplate().getCoupons() * vm.getGroupSize();
        if (isIsf()) {
            coupon = vm.getMinCostProviderPerFamily(getTemplate().getFamily()).getCoupons()
                    * vm.getGroupSize();
        }
        if (couponToBestVM.get(coupon) == null) {
            Deque<Pair<SMAVirtualMachine, Integer>> vmToIndexQueue =
                    new LinkedList<>();
            couponToBestVM.put(coupon, vmToIndexQueue);
        }
        if (addFirst) {
            couponToBestVM.get(coupon).addFirst(new Pair(vm, vmIndex));
        } else {
            couponToBestVM.get(coupon).addLast(new Pair(vm, vmIndex));
        }
        discountableVMs.add(vm);
    }

    /**
     * remove the 0th entry from couponToBestVM.get(coupon).
     * Side Effect: updates discountableVMs.
     *
     * @param coupon the key to search in couponToBestVM
     */
    public void removeVMFromCouponToBestVM(int coupon) {
        Deque<Pair<SMAVirtualMachine, Integer>> vmList = couponToBestVM.get(coupon);
        if (vmList != null) {
            if (vmList.isEmpty()) {
                couponToBestVM.remove(coupon);
            } else {
                Pair<SMAVirtualMachine, Integer> vmIndexPair = vmList.pop();
                SMAVirtualMachine vm = vmIndexPair.first;
                discountableVMs.remove(vm);
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
     * @param virtualMachineGroupMap map from group name to virtualMachine Group
     * @return the most preferred vm for the RI that the RI has not yet proposed and doesnt require more
     * coupons than what the RI has. In other words vm that can be fully covered by RI.
     */
    public Pair<SMAVirtualMachine, Integer> findBestVMIndexFromCouponToBestVM(float coupon,
                                                               Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap) {
        // all possible coupons are passed to compute bestPartialVM.
        Pair<SMAVirtualMachine, Integer> bestPartialVM = findBestVM(couponToBestVM.keySet());
        // only those coupons which are below "coupon" are passed. We need full coverage.
        Pair<SMAVirtualMachine, Integer> bestFullVM = findBestVM(couponToBestVM.keySet().stream()
                .filter(a -> a < coupon + SMAUtils.BIG_EPSILON).collect(Collectors.toSet()));
        if (bestFullVM.first == null) {
            return bestPartialVM;
        }
        if (bestPartialVM.first == null) {
            return bestFullVM;
        }
        if (compareCost(bestFullVM.first, bestPartialVM.first, virtualMachineGroupMap, coupon) > 0) {
            return bestPartialVM;
        } else {
            return bestFullVM;
        }
    }

    /**
     * Find the best VM  the RI can discount whose coupons requirement is in possibleCoupons set.
     * @param possibleCoupons the set of possible coupons.
     * @return the best VM the RI can discount.
     */
    public Pair<SMAVirtualMachine, Integer> findBestVM(Set<Integer> possibleCoupons) {
        SMAVirtualMachine bestVM = null;
        Integer bestIndex = Integer.MAX_VALUE;
        for (Integer c : possibleCoupons) {
            Deque<Pair<SMAVirtualMachine, Integer>> vmList = couponToBestVM.get(c);
            if (vmList != null && !vmList.isEmpty() && vmList.getFirst().second < bestIndex) {
                bestIndex = vmList.getFirst().second;
                bestVM = vmList.getFirst().first;
            }
        }
        return new Pair(bestVM, bestIndex);
    }

    /**
     * Compute the savings obtained by matching the virtual machine with reservedInstance.
     *
     * @param vm virtual machine of interest.
     * @param virtualMachineGroupMap  map from group name to virtualMachine Group
     * @param coupons remaining coupons available in the RI.
     * @return  the savings obtained by matching the virtual machine with reservedInstance.
     */
    public float computeSaving(SMAVirtualMachine vm,
                               Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                               float coupons) {
        final List<SMAVirtualMachine> vmList;
        if (vm.getGroupSize() > 1) {
            vmList = virtualMachineGroupMap.get(vm.getGroupName()).getVirtualMachines();
        } else {
            vmList = Collections.singletonList(vm);
        }
        SMATemplate riTemplate = getNormalizedTemplate();
        float netSavingvm = 0;
        for (SMAVirtualMachine member : vmList) {
            float onDemandCostvm = member.getNaturalTemplate().getNetCost(vm.getBusinessAccountId(), vm.getOsType(), 0);
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
            float afterMoveCostvm = isIsf() ? member.getMinCostProviderPerFamily(
                    riTemplate.getFamily()).getNetCost(vm.getBusinessAccountId(),
                            vm.getOsType(),
                            coupons / vm.getGroupSize()) : riTemplate
                    .getNetCost(vm.getBusinessAccountId(), vm.getOsType(), coupons / vm.getGroupSize());

            netSavingvm += (onDemandCostvm - afterMoveCostvm);
        }
        return netSavingvm;

    }

    /**
     * compare the saving per coupon of two virtual machines when discounted by this RI.
     * @param vm1 first VM
     * @param vm2 second VM
     * @param virtualMachineGroupMap  map from group name to virtualMachine Group
     * @param coupons remaining coupons available in the RI.
     * @return -1 if vm1 has more saving per coupon. 1 if vm2 has more saving per coupon. 0 otherwise.
     */
    public int compareCost(SMAVirtualMachine vm1, SMAVirtualMachine vm2,
                           Map<String, SMAVirtualMachineGroup> virtualMachineGroupMap,
                           float coupons) {
        float netSavingVm1 = computeSaving(vm1, virtualMachineGroupMap, coupons);
        float netSavingVm2 = computeSaving(vm2, virtualMachineGroupMap, coupons);

        String riFamily = getNormalizedTemplate().getFamily();
        float couponsVm1 = 0.0f;
        float couponsVm2 = 0.0f;
        if (isIsf()) {
            couponsVm1 = Math.min(coupons, vm1.getMinCostProviderPerFamily(riFamily).getCoupons() * vm1.getGroupSize());
            couponsVm2 = Math.min(coupons, vm2.getMinCostProviderPerFamily(riFamily).getCoupons() * vm2.getGroupSize());
        } else {
            couponsVm1 = (float)getNormalizedTemplate().getCoupons() * vm1.getGroupSize();
            couponsVm2 = (float)getNormalizedTemplate().getCoupons() * vm2.getGroupSize();
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
        return (vm.getCurrentRI() != null &&
            vm.getCurrentRI().getRiKeyOid() == getRiKeyOid()) &&
            vm.getCurrentRICoverage() > SMAUtils.EPSILON;
    }

    /**
     * Get list of accounts that RI can be applied to.
     *
     * @return List of accounts that RI can be applied to
     */
    public Set<Long> getApplicableBusinessAccounts() {
        return applicableBusinessAccounts;
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
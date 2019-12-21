package com.vmturbo.market.cloudvmscaling.entities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.tuple.MutablePair;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.market.cloudvmscaling.analysis.SMAUtils;

/**
 * Stable marriage algorithm representation of an RI.
 */

public class SMAReservedInstance {
    /*
     * unique identifier
     */
    private final long oid;
    /*
     * name of RI
     */
    private final String name;
    /*
     * BusinessAccount, subaccount of billing account.
     */
    private final long businessAccount;
    /*
     * Percentage of RI used to discount VMs.
     */
    private float utilization;
    /*
     * Template, used to infer CSP, family and coupons.
     */

    private final SMATemplate template;

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
        assume the ri can discount 5 vms (vma vmb vmc vmd vme) in this order and they
        need 2 4 1 2 1 coupons respectively. Then couponToBestVM will look like this.
           1 -> [(vmc,2),(vme,5)]
           2 -> [(vma,0),(vmd,4)]
           4 -> [(vmb,1)]

        This is done so that we can know the preference of ri within a coupon family.
        Also we can get the most preferred vm which needs < x coupons efficiently.
     */
    private HashMap<Integer, List<Pair<SMAVirtualMachine, Integer>>> couponToBestVM;

    private HashSet<SMAVirtualMachine> discountableVMs;

    /*
     * Availability zone.  If zone == NO_ZONE then regional RI.
     */
    private final long zone;

    /*
     * Total count is the count after we normalize the RI and combine the RIs.
     */
    private float totalCount;

    private final int count;

    /*
     * Reserved Instance is InstanceSizeFlexible or not based on context.
     */
    private final boolean isf;

    // list of RI that constitute this normalized RIs. along with the coupon of the RI already used.
    private List<MutablePair<SMAReservedInstance, Integer>> members;

    // map to keep track of RI Coverage for each group.
    // Saved to avoid recomputing everytime.
    // RI OID x Pair<couponsCovered, totalCoupons>
    private HashMap<Long, Pair<Float, Float>> riCoveragePerGroup;

    // last discounted VM
    private SMAVirtualMachine lastDiscountedVM;

    /**
     * SMA Reserved Instance.
     *
     * @param oid             unique identifier
     * @param name            name of RI
     * @param businessAccount business account
     * @param utilization     RI utilization
     * @param template        compute tier
     * @param zone            availabilty zone
     * @param count           number of RIs
     * @param context         context
     */
    public SMAReservedInstance(final long oid,
                               @Nonnull final String name,
                               final long businessAccount,
                               float utilization,
                               @Nonnull final SMATemplate template,
                               final long zone,
                               final int count,
                               @Nonnull final SMAContext context) {
        this.oid = oid;
        this.name = Objects.requireNonNull(name, "name is null!");
        this.businessAccount = businessAccount;
        this.utilization = utilization;
        this.template = Objects.requireNonNull(template, "template is null!");
        this.normalizedTemplate = template;
        this.zone = zone;
        this.totalCount = count;
        this.count = count;
        Objects.requireNonNull(context, "context is null!");
        this.isf = computeInstanceSizeFlexible(context);
        this.members = new ArrayList<>();
        couponToBestVM = new HashMap<>();
        lastDiscountedVM = null;
        riCoveragePerGroup = new HashMap<>();
        discountableVMs = new HashSet<>();
    }

    public SMAVirtualMachine getLastDiscountedVM() {
        return lastDiscountedVM;
    }

    public void setLastDiscountedVM(final SMAVirtualMachine lastDiscountedVM) {
        this.lastDiscountedVM = lastDiscountedVM;
    }

    /**
     * Finds if an Reserved Instance is InstanceSizeFlexible based on context.
     *
     * @param context the context the Reserved Instance belongs to.
     * @return true if the Reserved Instance is InstanceSizeFlexible
     */
    public boolean computeInstanceSizeFlexible(SMAContext context) {
        if (context.getCsp() == SMACSP.AZURE ||
                (context.getCsp() == SMACSP.AWS &&
                        context.getTenancy() == SMATenancy.DEFAULT &&
                        SMAUtils.LINUX_OS.contains(context.getOs()) &&
                        zone == SMAUtils.NO_ZONE)) {
            return true;
        }
        return false;
    }

    public boolean isIsf() {
        return isf;
    }

    /**
     * determines if the RI is regional or zonal.
     * @return returns true if the RI is regional. false if zonal.
     */
    public boolean isRegionScoped() {
        if (zone == SMAUtils.NO_ZONE) {
            return true;
        }
        return false;
    }

    @Nonnull
    public long getOid() {
        return oid;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public int getCount() {
        return count;
    }

    public HashSet<SMAVirtualMachine> getDiscountableVMs() {
        return discountableVMs;
    }

    public SMATemplate getTemplate() {
        return template;
    }

    @Nonnull
    public long getBusinessAccount() {
        return businessAccount;
    }

    public float getUtilization() {
        return utilization;
    }

    public void setUtilization(float value) {
        utilization = value;
    }

    @Nonnull
    public SMATemplate getNormalizedTemplate() {
        return normalizedTemplate;
    }

    public long getZone() {
        return zone;
    }

    public float getTotalCount() {
        return totalCount;
    }

    public void setNormalizedTemplate(final SMATemplate normalizedTemplate) {
        this.normalizedTemplate = normalizedTemplate;
    }

    public void setTotalCount(final float totalCount) {
        this.totalCount = totalCount;
    }

    public List<MutablePair<SMAReservedInstance, Integer>> getMembers() {
        return members;
    }

    /*
     * Determine if two RIs are equivalent.  They are equivalent if same template, zone and business account.
     * If the RIs are regional, then zone == NO_ZONE.
     */
    @Override
    public int hashCode() {
        return Objects.hash(normalizedTemplate.getOid(), zone, businessAccount);
    }

    /**
     * RI's that have same businessAccount,normalizedTemplate and zone should be combined. This allows
     * multiple RI's to discount a single VM in case of ISF. Multiple RI to discount multiple VMs in case
     * of ASG.
     */
    @Override
    public boolean equals(Object obj) {
        SMAReservedInstance ri = (SMAReservedInstance)obj;

        // If the object is compared with itself then return true
        if (ri == this) {
            return true;
        }

        if (businessAccount == ri.businessAccount &&
                normalizedTemplate.getOid() == ri.normalizedTemplate.getOid() &&
                zone == ri.zone) {
            return true;
        }
        return false;
    }

    /**
     * Normalize the RI to the cheapest template in the family for ISF RIs.
     *
     * @param familyNameToCheapestTemplate map from family to the cheapest template in the family
     */

    public void normalizeTemplate(Map<String, SMATemplate> familyNameToCheapestTemplate) {
        if (isIsf()) {
            SMATemplate oldTemplate = getNormalizedTemplate();
            SMATemplate newTemplate = familyNameToCheapestTemplate
                    .get(getNormalizedTemplate().getFamily());
            float countMultiplier = (float)oldTemplate.getCoupons() / (float)newTemplate.getCoupons();
            setTotalCount(getCount() * countMultiplier);
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
     * add  the pair (vm,vmIndex) to couponToBestVM.get(coupon).
     *
     * @param coupon  the key to search in couponToBestVM
     * @param vmIndex the index of vm in RI's preference list.
     * @param vm      the vm to be added.
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
            if (isSingleVMDiscounted(member)) {
                coverage = coverage + member.getCurrentRICoverage();
            }
            total = total + member.getCurrentTemplate().getCoupons();
        }
        Pair<Float, Float> pair = new Pair<>(coverage, total);
        riCoveragePerGroup.put(virtualMachineGroup.getOid(), pair);
    }

    /**
     * coupons vm  vmgroup is currently discounted by this RI.
     *
     * @param vm the vm to be checked
     * @return discounted coupons.
     */
    public float getRICoverage(SMAVirtualMachine vm) {
        if (vm.getGroupSize() > 1) {
            return riCoveragePerGroup.get(vm.getGroupOid()).first
                    / riCoveragePerGroup.get(vm.getGroupOid()).second;
        } else {
            if (isSingleVMDiscounted(vm)) {
                return vm.getCurrentRICoverage()
                        / vm.getCurrentTemplate().getCoupons();
            } else {
                return 0;
            }
        }
    }

    /**
     * checks if the vm is currently discounted by this RI.
     *
     * @param vm the vm to be checked
     * @return true if the vm is currently discounted.
     */

    public boolean isSingleVMDiscounted(SMAVirtualMachine vm) {
        float riCoverage = vm.getCurrentRICoverage();
        if (riCoverage > SMAUtils.EPSILON) {
            if (!isIsf()) {
                return (vm.getCurrentTemplate().getOid()
                        == getTemplate().getOid());
            } else {
                return (vm.getCurrentTemplate().getFamily()
                        .equals(getTemplate().getFamily()));
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "SMAReservedInstance{" +
                "OID='" + oid + "'" +
                ", name='" + name + "'" +
                ", businessAccount='" + businessAccount + "'" +
                ", utilization=" + utilization +
                ", normalizedTemplate=" + normalizedTemplate +
                ", zone='" + zone + "'" +
                ", totalCount=" + totalCount +
                '}';
    }
}
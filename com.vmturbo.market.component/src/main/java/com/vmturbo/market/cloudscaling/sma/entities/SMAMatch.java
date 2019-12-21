package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.auth.api.Pair;

/**
 * Stable Marriage Algorithm representation of a SMAOutput.
 * Specifies for a VM the target template and if there is any RI coverage.
 */
public class SMAMatch {
    /**
     * VirtualMachine.
     */
    private final SMAVirtualMachine virtualMachine;

    /**
     * Template.
     */
    private final SMATemplate template;

    /**
     * ReservedInstance.
     */
    private List<Pair<SMAReservedInstance, Integer>> memberReservedInstances;

    /**
     * Discounted Coupons.
     */
    private int discountedCoupons;

    /**
     * Constructor when the VM is matched to a single reserved instance.
     *
     * @param virtualMachine virtual machine
     * @param template the template the virtual machine is scaled to
     * @param reservedInstance the reserved instance the VM is associated with
     * @param discountedCoupons the number of coupons the ri discounts the vm
     */
    public SMAMatch(@Nonnull final SMAVirtualMachine virtualMachine,
                    @Nonnull final SMATemplate template,
                    final SMAReservedInstance reservedInstance,
                    final int discountedCoupons) {
        this.virtualMachine = Objects.requireNonNull(virtualMachine, "virtualMachine is null!");
        this.template = Objects.requireNonNull(template, "template is null!");
        this.discountedCoupons = discountedCoupons;
        if (reservedInstance == null) {
            this.memberReservedInstances = new ArrayList<>();
        } else {
            this.memberReservedInstances =
                    new ArrayList<>(Arrays.asList(new Pair<>(reservedInstance, discountedCoupons)));
        }

    }

    /**
     * Constructor when the VM is matched to multiple reserved instances.
     *
     * @param virtualMachine  virtual machine
     * @param template  the template the virtual machine is scaled to
     * @param discountedCoupons  the number of coupons the ri discounts the vm
     * @param memberReservedInstances the reserved instances the VM is associated with along
     *                                with actual coupons each ri is discounting.
     */
    public SMAMatch(@Nonnull final SMAVirtualMachine virtualMachine,
                    @Nonnull final SMATemplate template,
                    final int discountedCoupons,
                    final List<Pair<SMAReservedInstance, Integer>> memberReservedInstances) {
        this.virtualMachine = Objects.requireNonNull(virtualMachine, "virtualMachine is null!");
        this.template = Objects.requireNonNull(template, "template is null!");
        this.discountedCoupons = discountedCoupons;
        this.memberReservedInstances = memberReservedInstances;
    }

    // TODO add the netcost computation method

    @Nonnull
    public SMAVirtualMachine getVirtualMachine() {
        return virtualMachine;
    }

    @Nonnull
    public SMATemplate getTemplate() {
        return template;
    }

    /**
     * return the reserved instance associated with a match. call this only when we know that
     * the match is associated with a single RI.
     * @return the first reserved instance associated with the match.(Ideally there is only one)
     */
    public SMAReservedInstance getReservedInstance() {
        if (memberReservedInstances == null || memberReservedInstances.size() == 0) {
            return null;
        }
        return memberReservedInstances.get(0).first;
    }

    public int getDiscountedCoupons() {
        return discountedCoupons;
    }

    public void setDiscountedCoupons(final int discountedCoupons) {
        this.discountedCoupons = discountedCoupons;
    }

    public List<Pair<SMAReservedInstance, Integer>> getMemberReservedInstances() {
        return memberReservedInstances;
    }


    @Override
    public String toString() {
        return "SMAMatch{" +
                "virtualMachine=" + virtualMachine +
                ", template=" + template +
                ", memberReservedInstances=" + getMemberReservedInstances().size() +
                ", discountedCoupons=" + discountedCoupons +
                '}';
    }
}

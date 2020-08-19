package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Stable Marriage Algorithm representation of a SMAOutput.
 * Specifies for a VM the target template and if there is any RI coverage.
 */
public class SMAMatch {
    /**
     * VirtualMachine.
     */
    private SMAVirtualMachine virtualMachine;

    /**
     * Template.
     */
    private SMATemplate template;

    private SMAReservedInstance reservedInstance;

    /**
     * Discounted Coupons.
     */
    private int discountedCoupons;

    /**
     * ri-coverage after we do post-processing. Still coupons. Not percentage.
     */
    private float projectedRICoverage;

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
        this.reservedInstance = reservedInstance;
    }

    public float getProjectedRICoverage() {
        return projectedRICoverage;
    }

    public void setProjectedRICoverage(final float projectedRICoverage) {
        this.projectedRICoverage = projectedRICoverage;
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

    public void setTemplate(final SMATemplate template) {
        this.template = template;
    }

    public void setVirtualMachine(final SMAVirtualMachine virtualMachine) {
        this.virtualMachine = virtualMachine;
    }

    public void setReservedInstance(final SMAReservedInstance reservedInstance) {
        this.reservedInstance = reservedInstance;
    }

    /**
     * return the reserved instance associated with a match.
     * @return the reserved instance associated with the match.
     */
    @Nonnull
    public SMAReservedInstance getReservedInstance() {
        return reservedInstance;
    }

    public int getDiscountedCoupons() {
        return discountedCoupons;
    }

    public void setDiscountedCoupons(final int discountedCoupons) {
        this.discountedCoupons = discountedCoupons;
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("SMAMatch VM OID=").append(virtualMachine.getOid())
            .append(" name=").append(virtualMachine.getName())
            .append(" currentTemplate=").append(virtualMachine.getCurrentTemplate().getName())
            .append(" naturalTemplate=").append(virtualMachine.getNaturalTemplate().getName())
            .append(" projectedTemplate=").append(template.getName())
            .append(" coupons=").append(template.getCoupons());
        if (reservedInstance != null) {
                buffer.append( " RI OID=").append(reservedInstance.getOid())
                    .append(" template=").append(reservedInstance.getTemplate().getName())
                    .append(" coupons=").append(discountedCoupons);
        }
        return buffer.toString();
    }
}

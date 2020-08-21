package com.vmturbo.market.cloudscaling.sma.analysis;

/**
 * A class used to save the expected output for unittest purposes.
 */
public class SMAMatchTestTrim {

    private final Long virtualMachineOid;
    private final Long reservedInstanceOid;
    private final Long templateOid;
    private final Integer discountedCoupons;

    /**
     * constructor.
     *
     * @param virtualMachineOid   oid of the virtual machine.
     * @param reservedInstanceOid oid of the reserved instance.
     * @param templateOid            oid of the templateOid.
     * @param discountedCoupons   discounted coupons.
     */
    public SMAMatchTestTrim(Long virtualMachineOid,
                            Long reservedInstanceOid,
                            Long templateOid,
                            Integer discountedCoupons) {

        this.discountedCoupons = discountedCoupons;
        this.reservedInstanceOid = reservedInstanceOid;
        this.templateOid = templateOid;
        this.virtualMachineOid = virtualMachineOid;
    }

    public Integer getDiscountedCoupons() {
        return discountedCoupons;
    }

    public Long getReservedInstanceOid() {
        return reservedInstanceOid;
    }

    public Long getTemplateOid() {
        return templateOid;
    }

    public Long getVirtualMachineOid() {
        return virtualMachineOid;
    }
}

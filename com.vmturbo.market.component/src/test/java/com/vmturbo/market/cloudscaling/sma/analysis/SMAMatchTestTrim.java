package com.vmturbo.market.cloudscaling.sma.analysis;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;

/**
 * A class used to save the expected output for unittest purposes.
 */
public class SMAMatchTestTrim {

    private final Long virtualMachineOid;
    private final Long reservedInstanceOid;
    private final Long templateOid;
    private final CloudCommitmentAmount cloudCommitmentAmount;
    private final Float discountedCoupons;

    /**
     * constructor.
     *
     * @param virtualMachineOid   oid of the virtual machine.
     * @param reservedInstanceOid oid of the reserved instance.
     * @param templateOid            oid of the templateOid.
     * @param discountedCoupons   discounted coupons.
     * @param cloudCommitmentAmount the cloud commitment amount.
     */
    public SMAMatchTestTrim(Long virtualMachineOid,
                            Long reservedInstanceOid,
                            Long templateOid,
            CloudCommitmentAmount cloudCommitmentAmount,
            Float discountedCoupons) {

        this.discountedCoupons = discountedCoupons;
        this.reservedInstanceOid = reservedInstanceOid;
        this.templateOid = templateOid;
        this.virtualMachineOid = virtualMachineOid;
        this.cloudCommitmentAmount = cloudCommitmentAmount;
    }

    public CloudCommitmentAmount getCloudCommitmentAmount() {
        return cloudCommitmentAmount;
    }

    public Float getDiscountedCoupons() {
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

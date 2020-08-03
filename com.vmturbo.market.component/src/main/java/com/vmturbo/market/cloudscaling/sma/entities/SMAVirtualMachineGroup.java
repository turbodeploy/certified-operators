package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

/**
 *  SMAVirtualMachineGroup is used to capture a group of vms that are part of Auto scaling group.
 */

public class SMAVirtualMachineGroup {

    /*
     * Name of VM Group, unique per context
     */
    private final String name;

    /*
     * List of virtual machines in this group.
     */
    private List<SMAVirtualMachine> virtualMachines;

    private SMAVirtualMachine groupLeader;

    /*
     * a group is zonalDiscountable only if  the members belong to same zone.
     */
    private boolean zonalDiscountable;

    /**
     * Constructor for SMAVirtualMachineGroup.
     *
     * @param groupName the group name
     * @param virtualMachines the virtual machines that belong to this group.
     * @param groupProviderList the intersection of providers of all members
     */
    public SMAVirtualMachineGroup(@Nonnull final String groupName,
                                  List<SMAVirtualMachine> virtualMachines,
                                  List<SMATemplate> groupProviderList) {
        this.name = Objects.requireNonNull(groupName, "groupName is null!");
        this.virtualMachines = virtualMachines;
        Collections.sort(getVirtualMachines(), new SortByRiCoverageAndVmOID());
        // The first vm is set as groupLeader.
        this.groupLeader = getVirtualMachines().get(0);
        this.groupLeader.setGroupSize(getVirtualMachines().size());
        this.groupLeader.setGroupProviders(groupProviderList);
        this.zonalDiscountable = true;
        updateAllMembers();
    }

    /**
     * The group leader providers is the intersection of all providers in the group.
     * non leaders will have no provider and the group size 0.
     * if the virtual machines have different zone from the leader the group
     * is set zonalDiscountable false.
     */
    public void updateAllMembers() {
        for (SMAVirtualMachine virtualMachine : getVirtualMachines()) {
            if (virtualMachine != groupLeader) {
                // the non leaders have empty providers
                virtualMachine.setGroupProviders(Collections.emptyList());
                //non leaders have group size 0
                virtualMachine.setGroupSize(0);
                // update the group size of the leader with the size of the group.
                if (groupLeader.getZoneId() != virtualMachine.getZoneId()) {
                    this.zonalDiscountable = false;
                }
                virtualMachine.setNaturalTemplate(groupLeader.getNaturalTemplate());
                virtualMachine.setMinCostProviderPerFamily(groupLeader.getMinCostProviderPerFamily());
            }
        }
    }

    public String getName() {
        return name;
    }

    public List<SMAVirtualMachine> getVirtualMachines() {
        return virtualMachines;
    }

    public boolean isZonalDiscountable() {
        return zonalDiscountable;
    }

    public SMAVirtualMachine getGroupLeader() {
        return groupLeader;
    }

    /**
     *  Comparator to compare vms by OID.
     */
    public static class SortByRiCoverageAndVmOID implements Comparator<SMAVirtualMachine> {
        /**
         * given two VMs compare them by RI converage and then by oid.
         * @param vm1 first VM
         * @param vm2 second VM
         * @return -1 if vm1 has a higher RI coverage. 1 otherwise. higher oid is used to break ties.
         */
        @Override
        public int compare(SMAVirtualMachine vm1, SMAVirtualMachine vm2) {
            if (Math.round(vm1.getCurrentRICoverage()) - Math.round(vm2.getCurrentRICoverage()) > 0) {
                return -1;
            } else if (Math.round(vm1.getCurrentRICoverage()) - Math.round(vm2.getCurrentRICoverage()) < 0) {
                return 1;
            }
            return (vm1.getOid() - vm2.getOid() > 0) ? -1 : 1;
        }
    }
}

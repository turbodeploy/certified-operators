package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
     */
    public SMAVirtualMachineGroup(@Nonnull final String groupName,
                                  List<SMAVirtualMachine> virtualMachines) {
        this.name = Objects.requireNonNull(groupName, "groupName is null!");
        this.virtualMachines = virtualMachines;
        Collections.sort(getVirtualMachines(), new SortByRiCoverageAndVmOID());
        // The first vm is set as groupLeader.
        this.groupLeader = getVirtualMachines().get(0);
        this.groupLeader.setGroupSize(getVirtualMachines().size());
        this.zonalDiscountable = true;
        for (SMAVirtualMachine vm : getVirtualMachines()) {
            if (vm != groupLeader) {
                update(vm);
            }
        }
        groupLeader.updateNaturalTemplateAndMinCostProviderPerFamily();
        updateNaturalTemplateOfAllMembers();

    }

    /**
     * update the natural template of all members with the natural template of the leader.
     */
    private void updateNaturalTemplateOfAllMembers() {
        // this has to be set here because the providers of the non leader is empty.
        for (SMAVirtualMachine groupMember : getVirtualMachines()) {
            if (groupLeader.getGroupProviders().size() > 0) {
                groupMember.setNaturalTemplate(groupLeader.getNaturalTemplate());
            } else {
                groupMember.setNaturalTemplate(groupMember.getCurrentTemplate());
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
     * The group leader providers is the intersection of all providers in the group.
     * non leaders will have no provider and the group size 0.
     * if the virtual machines have different zone from the leader the group
     * is set zonalDiscountable false.
     *
     * @param virtualMachine the virtual machine updated
     */
    public void update(SMAVirtualMachine virtualMachine) {
        // update the groupLeader provider with the intersection of all member providers.
        groupLeader.setGroupProviders(groupLeader.getGroupProviders().stream()
                .filter(a -> virtualMachine.getGroupProviders().contains(a))
                .collect(Collectors.toList()));
        // the non leaders have empty providers
        virtualMachine.setGroupProviders(new ArrayList<>());
        //non leaders have group size 0
        virtualMachine.setGroupSize(0);
        // update the group size of the leader with the size of the group.
        if (groupLeader.getZone() != virtualMachine.getZone()) {
            this.zonalDiscountable = false;
        }
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

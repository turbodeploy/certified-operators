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
     * Unique Identifier
     */
    private final long oid;
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
     * @param oid the oid of the Virtual Machine Group
     * @param groupName the group name
     * @param virtualMachines the virtual machines that belong to this group.
     */
    public SMAVirtualMachineGroup(@Nonnull final long oid,
                                  @Nonnull final String groupName,
                                  List<SMAVirtualMachine> virtualMachines) {
        this.oid = Objects.requireNonNull(oid, "OID is null!");
        this.name = Objects.requireNonNull(groupName, "groupName is null!");
        this.virtualMachines = virtualMachines;
        Collections.sort(getVirtualMachines(), new SortByVMOID());
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

    public long getOid() {
        return oid;
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
    public static class SortByVMOID implements Comparator<SMAVirtualMachine> {
        /**
         * given two vms compare them by oid.
         * @param vm1 first vm
         * @param vm2 second vm
         * @return -1 if vm1 has a higher oid. 1 otherwise.
         */
        @Override
        public int compare(SMAVirtualMachine vm1, SMAVirtualMachine vm2) {
            return (vm1.getOid() - vm2.getOid() > 0) ? -1 : 1;
        }
    }
}

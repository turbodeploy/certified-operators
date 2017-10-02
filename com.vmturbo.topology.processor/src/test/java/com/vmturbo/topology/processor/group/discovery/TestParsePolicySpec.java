package com.vmturbo.topology.processor.group.discovery;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * Class for testing of parsing DRS rules into policy specs.
 */
public class TestParsePolicySpec {

    private DiscoveredPolicyInfoParser discoveredPolicyInfoParser;
    private final List<CommonDTO.GroupDTO> groups = new LinkedList<>();

    @Before
    public void init() {
        initGroups();
    }

    /**
     * Initializes list of groups for test. Creates groups and constraints.
     */
    public void initGroups() {
        groups.clear();
        // Constraint for the buyers of first policy
        CommonDTO.GroupDTO.ConstraintInfo constraint =
                createConstraint("firstPolicy", true);
        // Add buyers group of first policy
        groups.add(createGroup("group_1", constraint));
        // Constraint for the sellers group of first policy
        constraint = createConstraint("firstPolicy", false);
        // Add sellers group of first policy
        groups.add(createGroup("group_2", constraint));
        // This is a constraint without a pair. Policy shouldn't be parsed from it
        constraint = createConstraint("notPolicy", true);
        // Group with the constraint without a pair. Policy shouldn't be parsed from it
        groups.add(createGroup("group_3", constraint));
        // Constraint and group of the sellers of second policy
        constraint = createConstraint("secondPolicy", false);
        groups.add(createGroup("group_4", constraint));
        // Constraint and group of the buyers of
        constraint = createConstraint("secondPolicy", true);
        groups.add(createGroup("group_5", constraint));
    }

    /**
     * Tests parsing groupDTOs to UploadedPolicySpecs
     */
    @Test
    public void testParsePoliciesOfGroups() {

        discoveredPolicyInfoParser = new DiscoveredPolicyInfoParser(groups);
        List<DiscoveredPolicyInfo> policies =
                discoveredPolicyInfoParser.parsePoliciesOfGroups();
        // initGroups() creates 3 pair of groups, 2 of them are engaged by policy
        Assert.assertEquals(2, policies.size());
        Assert.assertEquals(DiscoveredPolicyInfoParser.createStringId(groups.get(0)), policies.get(0)
                .getBuyersGroupStringId());
        Assert.assertEquals(DiscoveredPolicyInfoParser.createStringId(groups.get(1)),policies.get
                (0).getSellersGroupStringId());
        Assert.assertEquals(groups.get(0).getConstraintInfo().getConstraintType().getNumber(),
                policies.get(0).getConstraintType());
        Assert.assertEquals(groups.get(1).getConstraintInfo().getConstraintType().getNumber(),
                policies.get(0).getConstraintType());
        Assert.assertEquals(groups.get(0).getConstraintInfo().getConstraintName(),
                policies.get(0).getPolicyName());
    }

    /**
     *  Tests parsing when groups has VM group which engaged with cluster by policy
     */
    @Test
    public void testParsePoliciesOfGroupsWithVmClusterPolicy() {
        // Add pair of Vm-cluster groups, engaged by BUYER_BUYER_AFFINITY. Should be parsed in test.
        groups.addAll(addVmClusterPolicyToTestedGroups
                ("affinityCluster", ConstraintType.BUYER_BUYER_AFFINITY));
        // Add pair of Vm-cluster groups, engaged by BUYER_BUYER_ANTI_AFFINITY. Should be parsed.
        groups.addAll(addVmClusterPolicyToTestedGroups
                ("antiAffinityCluster", ConstraintType.BUYER_BUYER_ANTI_AFFINITY));
        // Add pair of Vm-cluster groups, with non-policy affinity. Shouldn't be parse to Policy.
        groups.addAll(addVmClusterPolicyToTestedGroups
                ("nonPolicyCluster", ConstraintType.BUYER_SELLER_AFFINITY));
        discoveredPolicyInfoParser = new DiscoveredPolicyInfoParser(groups);
        List<DiscoveredPolicyInfo> policies =
                discoveredPolicyInfoParser.parsePoliciesOfGroups();

        //So, we has 2 vm-cluster pairs with Policy and 2 Policy pairs of groups from initGroups()
        Assert.assertEquals(4, policies.size());
    }

    /**
     *  Adds two more groups to group list. one of them is VM and second is cluster.
     * @return New list of tested groups
     */
    private List<CommonDTO.GroupDTO> addVmClusterPolicyToTestedGroups(String clusterName,
            ConstraintType vmGroupConstraintType) {

        List<CommonDTO.GroupDTO> testedGroups = new LinkedList<>();

        CommonDTO.GroupDTO.ConstraintInfo vmConstraint =
                createConstraint(clusterName, true)
                        .toBuilder().setConstraintType(vmGroupConstraintType).build();

        CommonDTO.GroupDTO vmGroup = createGroup("VM_group", vmConstraint)
                .toBuilder().setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE)
                .setDisplayName("vm/" + clusterName + "/vmturbo.com").build();

        CommonDTO.GroupDTO.ConstraintInfo clusterConstraint =
                createConstraint(clusterName + "_cluster", false).toBuilder()
                .setConstraintType(CommonDTO.GroupDTO.ConstraintType.CLUSTER).build();

        CommonDTO.GroupDTO clusterGroup = createGroup(clusterName, clusterConstraint)
                .toBuilder().setEntityType(CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE)
                .setDisplayName(clusterName).build();

        testedGroups.add(vmGroup);
        testedGroups.add(clusterGroup);
        return testedGroups;
    }

    /**
     * Creates group for testing
     * @param id name of created group
     * @param constraintInfo constraint info of created group
     * @return created group
     */
    private static CommonDTO.GroupDTO createGroup(String id, CommonDTO.GroupDTO.ConstraintInfo constraintInfo) {
        return CommonDTO.GroupDTO.newBuilder()
                .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE)
                .setSourceGroupId(id)
                .setConstraintInfo(constraintInfo).build();
    }

    /**
     * Creates constraint info for tested group
     * @param constraintId
     * @param isBuyer
     * @return
     */
    private static CommonDTO.GroupDTO.ConstraintInfo createConstraint(String constraintId, boolean isBuyer) {
        return CommonDTO.GroupDTO.ConstraintInfo.newBuilder()
                .setConstraintName(constraintId)
                .setConstraintType(CommonDTO.GroupDTO.ConstraintType.BUYER_SELLER_AFFINITY)
                .setConstraintId(constraintId)
                .setIsBuyer(isBuyer).build();
    }
}

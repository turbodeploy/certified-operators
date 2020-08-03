package com.vmturbo.topology.processor.group.discovery;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintInfo;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.ConstraintType;
import com.vmturbo.platform.common.dto.CommonDTO.UpdateType;

/**
 * Class for testing of parsing DRS rules into policy specs.
 */
public class ParsePolicySpecTest {

    private DiscoveredPolicyInfoParser discoveredPolicyInfoParser;

    private static final long TARGET_ID = 111L;

    private final static String POLICY_ID_1 = "policy-1";
    private final static String POLICY_ID_2 = "policy-2";
    private final static String POLICY_ID_3 = "policy-3";
    private final static String POLICY_ID_4 = "non-policy-4";

    /**
     * Creates groups with buyer/seller constraints.
     *
     * @return list of groups associated with buyer/seller constraints
     */
    private List<CommonDTO.GroupDTO> initBuyerSellerGroups() {
        List<CommonDTO.GroupDTO> groups = new LinkedList<>();
        // Constraint and buyers group for policy 1
        groups.add(createGroup("group_1", createConstraint(POLICY_ID_1, true)));
        // Constraint and sellers group for policy 1
        groups.add(createGroup("group_2", createConstraint(POLICY_ID_1, false)));

        // A constraint with just one group. This should not become a policy.
        groups.add(createGroup("group_3", createConstraint(POLICY_ID_2, true)));

        // Constraint and sellers group for policy 3
        groups.add(createGroup("group_4", createConstraint(POLICY_ID_3, false)));
        // Constraint and buyers group for policy 3
        groups.add(createGroup("group_5", createConstraint(POLICY_ID_3, true)));

        // Constraint and sellers group ofor policy 4. Group is deleted so this should not become a policy.
        groups.add(createGroup("group_6", createConstraint(POLICY_ID_4, false), true)); // deleted
        // Constraint and group of the buyers of policy 4
        groups.add(createGroup("group_7", createConstraint(POLICY_ID_4, true)));

        return groups;
    }

    /**
     * Tests parsing VM/PM policies.
     */
    @Test
    public void testParsePoliciesOfGroups() {

        List<CommonDTO.GroupDTO> groups = initBuyerSellerGroups();
        discoveredPolicyInfoParser = new DiscoveredPolicyInfoParser(groups, TARGET_ID);
        List<DiscoveredPolicyInfo> policies = discoveredPolicyInfoParser.parsePoliciesOfGroups();
        // 4 sets of groups created, only 2 of them become policies.
        // Of the other two - one is not a pair and one has a deleted group.
        Assert.assertEquals(2, policies.size());

        Map<String, CommonDTO.GroupDTO> stringIdToGroup = groups.stream()
            .collect(Collectors.toMap(GroupProtoUtil::createIdentifyingKey, Function.identity()));

        for (DiscoveredPolicyInfo policyInfo : policies) {
            CommonDTO.GroupDTO buyers = stringIdToGroup.get(policyInfo.getBuyersGroupStringId());
            CommonDTO.GroupDTO sellers = stringIdToGroup.get(policyInfo.getSellersGroupStringId());
            Assert.assertEquals(policyInfo.getPolicyName(),
                            sellers.getConstraintInfo().getConstraintId());
            Assert.assertEquals(policyInfo.getPolicyName(),
                            buyers.getConstraintInfo().getConstraintId());
            Assert.assertEquals(policyInfo.getConstraintType(),
                            buyers.getConstraintInfo().getConstraintType().getNumber());
            Assert.assertEquals(policyInfo.getConstraintType(),
                            sellers.getConstraintInfo().getConstraintType().getNumber());
        }
    }

    /**
     * Creates groups with buyer/buyer constraints.
     *
     * @return list of groups associated with buyer/seller constraints
     */
    private List<CommonDTO.GroupDTO> initBuyerBuyerGroups() {
        List<CommonDTO.GroupDTO> groups = new LinkedList<>();
        // Add a VM group with BUYER_BUYER_AFFINITY constraint. Should become a policy.
        groups.add(vmGroup("affinity", ConstraintType.BUYER_BUYER_AFFINITY));
        // Add a VM group with BUYER_BUYER_ANTI_AFFINITY constraint. Should become a policy.
        groups.add(vmGroup("anti-affinity", ConstraintType.BUYER_BUYER_ANTI_AFFINITY));
        // Add a VM group with buyer/seller constraint. Should not become a policy.
        groups.add(vmGroup("affinity-2", ConstraintType.BUYER_BUYER_AFFINITY, true));
        // Add the cluster group.
        groups.add(clusterGroup);
        return groups;
    }

    /**
     *  Test parsing VM/VM policies.
     */
    @Test
    public void testParsePoliciesOfGroupsWithVmClusterPolicy() {
        List<CommonDTO.GroupDTO> groups = initBuyerBuyerGroups();
        discoveredPolicyInfoParser = new DiscoveredPolicyInfoParser(groups, TARGET_ID);
        List<DiscoveredPolicyInfo> policies = discoveredPolicyInfoParser.parsePoliciesOfGroups();
        // 3 VM groups created with buyer/buyer constraints, only 2 of them become policies.
        // The third is a deleted group so no policy gets created.
        Assert.assertEquals(2, policies.size());

        Map<String, CommonDTO.GroupDTO> stringIdToGroup = groups.stream()
                .collect(Collectors.toMap(GroupProtoUtil::createIdentifyingKey, Function.identity()));
        for (DiscoveredPolicyInfo policyInfo : policies) {
            CommonDTO.GroupDTO buyers = stringIdToGroup.get(policyInfo.getBuyersGroupStringId());
            Assert.assertEquals(policyInfo.getPolicyName(),
                            buyers.getConstraintInfo().getConstraintId());
            Assert.assertEquals(policyInfo.getConstraintType(),
                            buyers.getConstraintInfo().getConstraintType().getNumber());
        }
    }

    /**
     * Creates groups without constraints.
     *
     * @return list of groups without constraints
     */
    private List<CommonDTO.GroupDTO> initGroupsWithoutConstraints() {
        List<CommonDTO.GroupDTO> groups = new LinkedList<>();
        groups.add(createGroupWithoutConstraints("group_1"));
        groups.add(createGroupWithoutConstraints("group_2"));

        return groups;
    }

    /**
     * Tests parsing groups without constraints - no policies should be created.
     */
    @Test
    public void testParsePoliciesOfGroupsWithoutConstraints() {

        List<CommonDTO.GroupDTO> groups = initGroupsWithoutConstraints();
        discoveredPolicyInfoParser = new DiscoveredPolicyInfoParser(groups, TARGET_ID);
        List<DiscoveredPolicyInfo> policies = discoveredPolicyInfoParser.parsePoliciesOfGroups();

        Assert.assertTrue(policies.isEmpty());
    }

    // THE cluster used for buyer/buyer groups.
    private static final String CLUSTER_NAME = "CLUSTER-1";
    private static final CommonDTO.GroupDTO clusterGroup = CommonDTO.GroupDTO.newBuilder()
                    .setEntityType(CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE)
                    .setDisplayName(CLUSTER_NAME)
                    .setConstraintInfo(ConstraintInfo.newBuilder()
                        .setConstraintType(ConstraintType.CLUSTER)
                        .setConstraintId("cluster-constraint")
                        .setConstraintName("foo")
                        .build())
                    .build();

    private static int count = 0; // used to generate distinct display names for VM groups

    /**
     *  Create a non-deleted VM group with a constraint associated with the cluster.
     *
     * @param constraintId constraint identifier
     * @param constraintType type of constraint to use with the VM group
     * @return a non-deleted VM group
     */
    private CommonDTO.GroupDTO vmGroup(String constraintId, ConstraintType constraintType) {
        return vmGroup(constraintId, constraintType, false);
    }

    /**
     *  Create a VM group with a constraint associated with THE cluster.
     *
     * @param constraintId constraint identifier
     * @param constraintType type of constraint to use with the VM group
     * @param deleted when true, mark the group as deleted
     * @return a VM group with the specified properties
     */
    private CommonDTO.GroupDTO vmGroup(String constraintId, ConstraintType constraintType,
                    boolean deleted) {
        CommonDTO.GroupDTO.ConstraintInfo vmConstraint =
                createConstraint(constraintId, true)
                        .toBuilder().setConstraintType(constraintType).build();
        CommonDTO.GroupDTO.Builder vmGroupBuilder = createGroup("VM_group", vmConstraint)
                .toBuilder().setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE)
                .setDisplayName("vm_" + (count++) + "/" + CLUSTER_NAME + "/vmturbo.com");
        if (deleted) {
            vmGroupBuilder.setUpdateType(UpdateType.DELETED);
        }
        return vmGroupBuilder.build();
    }

    /**
     * Creates a non-deleted group for testing.
     *
     * @param id name of created group
     * @param constraintInfo constraint info of created group
     * @return created group
     */
    private static CommonDTO.GroupDTO createGroup(String id, CommonDTO.GroupDTO.ConstraintInfo constraintInfo) {
        return createGroup(id, constraintInfo, false);
    }

    /**
     * Creates a group for testing.
     *
     * @param id name of created group
     * @param constraintInfo constraint info of created group
     * @param deleted when true, mark the group as deleted
     * @return created group
     */
    private static CommonDTO.GroupDTO createGroup(String id, CommonDTO.GroupDTO.ConstraintInfo constraintInfo, boolean deleted) {
        CommonDTO.GroupDTO.Builder groupBuilder = CommonDTO.GroupDTO.newBuilder()
                .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE)
                .setDisplayName(id)
                .setSourceGroupId(id)
                .setConstraintInfo(constraintInfo);
        if (deleted) {
            groupBuilder.setUpdateType(UpdateType.DELETED);
        }
        return groupBuilder.build();
    }

    /**
     * Creates a group without constraints for testing.
     *
     * @param id name of created group
     * @return created group
     */
    private static CommonDTO.GroupDTO createGroupWithoutConstraints(String id) {
        CommonDTO.GroupDTO.Builder groupBuilder = CommonDTO.GroupDTO.newBuilder()
                .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE)
                .setDisplayName(id)
                .setSourceGroupId(id);

        return groupBuilder.build();
    }

    /**
     * Creates a BUYER_SELLER_AFFINITY constraint info for tested group.
     *
     * @param constraintId constraint identifier
     * @param isBuyer whether the constraint info created is referenced from the
     *           buyers group or the sellers group
     * @return a constraint info with the provided arguments
     */
    private static CommonDTO.GroupDTO.ConstraintInfo createConstraint(String constraintId, boolean isBuyer) {
        return CommonDTO.GroupDTO.ConstraintInfo.newBuilder()
                .setConstraintName(constraintId)
                .setConstraintDisplayName(constraintId)
                .setConstraintType(CommonDTO.GroupDTO.ConstraintType.BUYER_SELLER_AFFINITY)
                .setConstraintId(constraintId)
                .setIsBuyer(isBuyer)
                .build();
    }
}

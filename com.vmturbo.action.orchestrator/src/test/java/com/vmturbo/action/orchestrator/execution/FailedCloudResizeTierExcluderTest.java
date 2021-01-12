package com.vmturbo.action.orchestrator.execution;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.setting.SettingProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.core.ErrorHandler;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.System;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Tests for logic that handles failed cloud VM resizes.
 */
public class FailedCloudResizeTierExcluderTest {
    // Constants
    private final long ACCOUNT_ID = 123456L;
    private final long GOOD_VM_ID = 1L;
    private final long MISSING_ORIGIN_VM_ID = 2L;
    private final long MISSING_ACCOUNT_VM_ID = 3L;
    private final long AZ_VM_ID = 4L;
    private final long OTHER_VM_ID = 5L;
    private final long DUPLICATE_VM_ID = 6L;
    private final long VM_NO_GROUP_1_ID = 7L;
    private final long VM_NO_GROUP_2_ID = 8L;
    private final long MISSING_VM_ID = 9L;
    private final long AZ_QF_VM_ID = 10L;
    private final long GOOD_VM_B_ID = 11L;
    private final long UNKNOWN_REGION_VM_ID = 210L;
    private final long REGION_A_ID = 401L;
    private final long REGION_B_ID = 402L;
    private final long AZ_A_ID = 403L;
    private final long UNKNOWN_REGION_ID = 499L;
    private final long OTHER_TIER_ID = 599L;
    private long UNKNOWN_TIER_ID = 0L; // will be set to an unused OID in createComputeTiers.

    private final long GOOD_VM_GROUP_ID = 201L;
    private final String GOOD_VM_GROUP_NAME =
            "good-vm/a0689cde-09b0-4236-a6bb-da80e6ecf9f7 - Failed execution Tier Family Exclusion Group (account 123456)";
    private final long GOOD_VM_B_GROUP_ID = 203L;
    private final String GOOD_VM_B_GROUP_NAME =
            "good-vm-region-b/a0689cde-09b0-4236-a6bb-da80e6ecf9f7 - Failed execution Tier Family Exclusion Group (account 123456)";
    private final long OTHER_GROUP_ID = 205L;
    private final String OTHER_GROUP_NAME = "other-vm/a0689cde-09b0-4236-a6bb-da80e6ecf9f7 - Failed execution Tier Family Exclusion Group (account 123456)";
    private static final String VM_NO_GROUP_2_NAME =
            "vm-in-no-group-2/a0689cde-09b0-4236-a6bb-da80e6ecf9f7 - Failed execution Tier Family Exclusion Group (account 123456)";
    private final long AZ_VM_GROUP_ID = 202L;
    private static final String VM_IN_AZ_GROUP_NAME =
            "vm-in-az/a0689cde-09b0-4236-a6bb-da80e6ecf9f7 - Failed execution Tier Family Exclusion Group (account 123456)";

    private final String REGION_A_POLICY_NAME =
            "Region Region-A - Failed execution Tier Family Exclusion Policy (account 123456)";
    private final long REGION_A_POLICY_ID = 301L;
    private final String REGION_B_POLICY_NAME =
            "Region Region-B - Failed execution Tier Family Exclusion Policy (account 123456)";
    private final long REGION_B_POLICY_ID = 302L;

    private String failedActionPatterns = "Action failed\nFailure .* occurred";
    private String badFailedActionPatterns = "Action failed\nFailure (.* occurred";

    // Service initialization
    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());
    private final SettingPolicyServiceMole testSettingPolicyService =
            spy(new SettingPolicyServiceMole());
    private final RepositoryServiceMole testRepositoryService = spy(new RepositoryServiceMole());

    /**
     * gRPC mocking support.
      */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(testGroupService,
            testSettingPolicyService, testRepositoryService);
    private GroupServiceBlockingStub groupServiceRpc;
    private RepositoryServiceBlockingStub repositoryService;
    private SettingPolicyServiceBlockingStub settingPolicyService;
    private FailedCloudVMGroupProcessor failedCloudVMGroupProcessor;

    private ActionFailure DEFAULT_ACTION_FAILURE = ActionFailure.newBuilder()
            .setErrorDescription("Failure foobar occurred")
            .setActionId(100L).build();

    private final TopologyEntityDTO GOOD_VM = createVm("good-vm", GOOD_VM_ID, true, REGION_A_ID);
    private final TopologyEntityDTO GOOD_VM_B = createVm("good-vm-region-b", GOOD_VM_B_ID, true, REGION_B_ID);
    private final TopologyEntityDTO AZ_VM = createVm("vm-in-az", AZ_VM_ID, true, AZ_A_ID);
    private final TopologyEntityDTO AZ_QF_VM = createVm("vm-in-az", AZ_QF_VM_ID, true, AZ_A_ID);
    private final TopologyEntityDTO DUPLICATE_VM = createVm("vm-in-az", DUPLICATE_VM_ID, true, AZ_A_ID);
    private final TopologyEntityDTO VM_NO_GROUP_1 = createVm("vm-in-no-group-1", VM_NO_GROUP_1_ID, true, REGION_A_ID);
    // VM in Region-B.  Region-B has no existing policy defined.
    private final TopologyEntityDTO VM_NO_GROUP_2 = createVm("vm-in-no-group-2", VM_NO_GROUP_2_ID, true, REGION_B_ID);
    private final TopologyEntityDTO OTHER_VM = createVm("other-vm", OTHER_VM_ID, true, REGION_B_ID);
    private final TopologyEntityDTO MISSING_ORIGIN_VM = createVm("no-origin", MISSING_ORIGIN_VM_ID, true);
    private final TopologyEntityDTO MISSING_ACCOUNT_VM = createVm("no-account", MISSING_ACCOUNT_VM_ID, false);
    private final TopologyEntityDTO UNKNOWN_REGION_VM = createVm("bad-region", UNKNOWN_REGION_VM_ID,
            true, UNKNOWN_REGION_ID);

    private final TopologyEntityDTO REGION_A = createEntity(EntityType.REGION, "Region-A", REGION_A_ID).build();
    private final TopologyEntityDTO REGION_B = createEntity(EntityType.REGION, "Region-B", REGION_B_ID).build();
    private final TopologyEntityDTO AZ_A = createEntity(EntityType.AVAILABILITY_ZONE,
            "AvailabilityZone-A", AZ_A_ID).build();
    private final Map<String, Long> tierNameToOid = new HashMap<>();
    private final List<TopologyEntityDTO> COMPUTE_TIERS = createComputeTiers();
    private final Long TIER_A1_OID = tierNameToOid.get("Tier-A1");
    private final Long TIER_A2_OID = tierNameToOid.get("Tier-A2");
    private final Long TIER_B1_OID = tierNameToOid.get("Tier-B1");
    private final Long TIER_AZ1_OID = tierNameToOid.get("Tier-Azure-1");
    private final Long TIER_AZ2_OID = tierNameToOid.get("Tier-Azure-2");

    TestOutputAppender output = new TestOutputAppender();

    /**
     * Initialization common to all tests.
     * @throws Exception should not happen.
     */
    @Before
    public void setUp() throws Exception {
        // Add a logging appender so that I can capture logging output
        ((org.apache.logging.log4j.core.Logger)FailedCloudResizeTierExcluder.logger)
                .addAppender(output);
        output.clear();
        groupServiceRpc = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        repositoryService =
                RepositoryServiceGrpc.newBlockingStub(testServer.getChannel());
        settingPolicyService = SettingPolicyServiceGrpc
                .newBlockingStub(testServer.getChannel());
        failedCloudVMGroupProcessor = createFailedCloudVMGroupProcessor(failedActionPatterns);
        initializeRepositoryService();
        initializeGroupService();
        initializeSettingPolicyService();
    }

    /**
     * Remove logging interceptor.
     */
    @After
    public void tearDown() {
        ((org.apache.logging.log4j.core.Logger)FailedCloudResizeTierExcluder.logger)
                .removeAppender(output);
    }

    /**
     * Ensure that the services have been invoked the required number of times.
     * @param repositoryCount number of times the repository service should be called
     * @param groupSearchCount number of times the group service should be called to Search
     * @param groupCreateCount number of times the group service should be called to Create
     * @param groupDeleteCount number of times the group service should be called to Delete
     * @param policySearchCount number of times the policy service should be called to Search
     * @param policyCreateCount number of times the policy service should be called to Create
     */
    private void verifyServiceAccesses(int repositoryCount,
                                      int groupSearchCount,
                                      int groupCreateCount,
                                      int groupDeleteCount,
                                      int policySearchCount,
                                      int policyCreateCount) {
        verify(testRepositoryService, times(repositoryCount)).retrieveTopologyEntities(any());
        verify(testGroupService, times(groupSearchCount)).getGroups(any());
        verify(testGroupService, times(groupCreateCount)).createGroup(any());
        verify(testGroupService, times(groupDeleteCount)).deleteGroup(any());
        verify(testSettingPolicyService, times(policySearchCount)).getSettingPolicy(any());
        verify(testSettingPolicyService, times(policyCreateCount)).createSettingPolicy(any());
    }

    /**
     * Verify that the test output contains a string.  If the string doesn't exist, then an
     * assertion error will be generated.
     * @param strings patterns to locate in the test output.  If multiple patterns are supplied, they
     *               must occur in the output in order.
     */
    private void verifyOutput(String... strings) {
        int patternIndex = 0;
        for (String line : output.getMessages()) {
            if (line.contains(strings[patternIndex])) {
                patternIndex += 1;
                if (patternIndex == strings.length) {
                    // Matched all requested patterns
                    return;
                }
            }
        }
        Assert.fail("Expecting in output: " + strings[patternIndex]);
    }

    /**
     * Verify that invalid regular expressions are handled properly.
     */
    @Test
    public void testFailedPatternParsing() {
        FailedCloudVMGroupProcessor gp = createFailedCloudVMGroupProcessor(badFailedActionPatterns);
        List<Pattern> patterns = gp.getFailedCloudResizeTierExcluder().getFailedActionPatterns();
        Assert.assertEquals(1, patterns.size());
        Assert.assertEquals("Action failed", patterns.get(0).pattern());
    }

    /**
     * Verify that valid regular expressions are handled properly.
     */
    @Test
    public void testSuccessfulPatternParsing() {
        List<Pattern> patterns = failedCloudVMGroupProcessor
                .getFailedCloudResizeTierExcluder().getFailedActionPatterns();
        Assert.assertEquals(2, patterns.size());
    }

    /**
     * Verify that poorly formed actions are handled correctly.
     */
    @Test
    public void invalidActionFormat() {
        // Null action
        Action action = createAction(101L, GOOD_VM_ID, EntityType.COMPUTE_TIER, TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, null);
        // If the action is invalid, we will not access any services
        verifyServiceAccesses(0, 0, 0, 0, 0, 0);
        verifyOutput("Not creating tier exclusion policy for action 1");
    }

    /**
     * Missing move information in action.
     */
    @Test
    public void missingMove() {
        // Create an action with no move.
        Action action = createAction(101L, GOOD_VM_ID, null, null);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyOutput("Missing move info");
    }

    /**
     * Verify that successful actions are not processed.
     */
    @Test
    public void testSuccessfulAction() {
        Action action = createAction(101L, GOOD_VM_ID, EntityType.COMPUTE_TIER, TIER_A1_OID);
        // Action was successful
        ActionFailure actionFailure = ActionFailure.newBuilder()
                .setErrorDescription("Action succeeded")
                .setActionId(100L).build();
        failedCloudVMGroupProcessor.handleActionFailure(action, actionFailure);
        // If the action is successful, we will not access any services
        verifyServiceAccesses(0, 0, 0, 0, 0, 0);
        verifyOutput("Not creating tier exclusion policy for action 101: Action is successful");
    }

    /**
     * Verify that failed actions are processed.
     */
    @Test
    public void failedAction() {
        Action action = createAction(101L, GOOD_VM_ID, EntityType.COMPUTE_TIER, TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyOutput("Creating exclusion policy for failed action ID 101");
    }

    /**
     * Ensures that a VM is present and has all required fields.
     */
    @Test
    public void testMissingVm() {
        // VM doesn't exist
        Action action = createAction(101L, MISSING_VM_ID, EntityType.COMPUTE_TIER, TIER_A2_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyServiceAccesses(1, 0, 0, 0, 0, 0);
        verifyOutput("Cannot find VM with OID");
    }

    /**
     * Multiple VMs with same OID.
     */
    @Test
    public void testDuplicateVm() {
        // OID returns multiple VM results
        Action action = createAction(101L, DUPLICATE_VM_ID, EntityType.COMPUTE_TIER, TIER_A2_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyOutput("Multiple entities with OID");
        verifyServiceAccesses(1, 0, 0, 0, 0, 0);
    }

    /**
     * Missing origin in VM.
     */
    @Test
    public void testMissingOriginVm() {
        // The VM is malformed, so handleActionFailure will return before accessing any services
        Action action = createAction(102L, MISSING_ORIGIN_VM_ID, EntityType.COMPUTE_TIER, TIER_A2_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyServiceAccesses(2, 0, 0, 0, 0, 0);
    }

    /**
     * Cannot locate account ID in VM.
     */
    @Test
    public void testMissingAccountVm() {
        // The VM is malformed, so handleActionFailure will return before accessing any services
        Action action = createAction(102L, MISSING_ACCOUNT_VM_ID, EntityType.COMPUTE_TIER, TIER_A2_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyServiceAccesses(2, 0, 0, 0, 0, 0);
        verifyOutput("Not creating tier exclusion policy for action 102: Cannot find account ID");
    }

    /**
     * Missing destination compute tier.
     */
    @Test
    public void testNoPeerSKUs() {
        Action action = createAction(101L, 1L, EntityType.REGION, 2L);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);

        // No policy or group should have been created for this.
        // No group or policy queries or creates or deletes.
        verifyServiceAccesses(1, 0, 0, 0, 0, 0);
        verifyOutput(
                "Cannot create template exclusion: missing destination compute tier in action",
                "Not creating tier exclusion policy for action 101: Cannot find peer SKUs"
        );
    }

    /**
     * Cannot determine VM's region.
     */
    @Test
    public void testCannotFindRegion() {
        Action action = createAction(101L, UNKNOWN_REGION_VM_ID, EntityType.COMPUTE_TIER,
                TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);

        // No policy or group should have been created for this.
        // No group or policy queries or creates or deletes.
        verifyServiceAccesses(3, 0, 0, 0, 0, 0);
        verifyOutput(
                "Cannot determine region name for VM bad-region - skipping creating family exclusion policy",
                "Not creating tier exclusion policy for action 101: Cannot find region"
        );
    }

    /**
     * Cannot find peer SKUs in current family.
     */
    @Test
    public void testPeerSKUs() {
        Action action = createAction(101L, UNKNOWN_REGION_VM_ID, EntityType.COMPUTE_TIER,
                UNKNOWN_TIER_ID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);

        // No policy or group should have been created for this.
        // No group or policy queries or creates or deletes.
        verifyServiceAccesses(2, 0, 0, 0, 0, 0);
        verifyOutput(
                "Cannot find tier family for tier ID 506",
                "Not creating tier exclusion policy for action 101: Cannot find peer SKUs"
        );
    }

    /**
     * Cannot determine region.
     */
    @Test
    public void testCannotDetermineRegion() {
        Action action = createAction(101L, UNKNOWN_REGION_VM_ID, EntityType.COMPUTE_TIER,
                UNKNOWN_TIER_ID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);

        // No policy or group should have been created for this.
        // No group or policy queries or creates or deletes.
        verifyServiceAccesses(2, 0, 0, 0, 0, 0);
        verifyOutput(
                "Cannot find tier family for tier ID 506",
                "Not creating tier exclusion policy for action 101: Cannot find peer SKUs"
        );
    }

    /**
     * Cannot create group policy.
     */
    @Test
    public void testNewGroupNewPolicyFailed() {
        // Fail policy creation
        when(testSettingPolicyService.createSettingPolicy(any()))
                .thenReturn(CreateSettingPolicyResponse.newBuilder().build());

        Action action = createAction(101L, VM_NO_GROUP_2_ID, EntityType.COMPUTE_TIER,
                TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyServiceAccesses(3, 1, 1, 1, 1, 1);
        verifyOutput(
                "Created group vm-in-no-group-2",
                "Creating new family exclusion policy for VM vm-in-no-group-2",
                "Failed to create tier exclusion policy",
                "Deleting newly-created group 'vm-in-no-group-2"
        );
    }

    /**
     * Cannot create group.
     */
    @Test
    public void testCannotCreateGroup() {
        Action action = createAction(101L, VM_NO_GROUP_1_ID, EntityType.COMPUTE_TIER,
                TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyServiceAccesses(3, 1, 1, 0, 0, 0);
        verifyOutput(
                "Could not create tier family exclusion group 'vm-in-no-group-1"
        );
    }

    /**
     * Recovery from group creation due to exception.
     */
    @Test
    public void testCannotCreateGroupDueToException() {
        Action action = createAction(101L, OTHER_VM_ID, EntityType.COMPUTE_TIER,
                TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyServiceAccesses(3, 1, 1, 0, 0, 0);
        verifyOutput(
                "Could not create tier family exclusion group 'other-vm/a0689cde-09b0-4236-a6bb-da80e6ecf9f7 - Failed execution Tier Family Exclusion Group (account 123456)': UNKNOWN"
        );
    }

    /**
     * Recovery from policy operation due to exception.
     */
    @Test
    public void testPolicyException() {
        Action action = createAction(101L, GOOD_VM_B_ID, EntityType.COMPUTE_TIER,
                TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyServiceAccesses(3, 1, 0, 0, 1, 1);
        verifyOutput(
                "Creating new family exclusion policy for VM good-vm-region-b with name 'Region Region-B - Failed execution Tier Family Exclusion Policy (account 123456)'",
                "Could not create or update tier exclusion policy 'Region Region-B - Failed execution Tier Family Exclusion Policy (account 123456)': UNKNOWN",
                "Failed to create tier exclusion policy 'Region Region-B - Failed execution Tier Family Exclusion Policy (account 123456)'"
        );
    }

    /**
     * Test finding Azure peer SKUs based on quotaFamily.
     */
    @Test
    public void testQuotaFamilyPeerSKUs() {
        Action action = createAction(101L, AZ_QF_VM_ID, EntityType.COMPUTE_TIER,
                TIER_AZ2_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyServiceAccesses(3, 1, 1, 0, 0, 0);
        verifyOutput(
                "Peer tiers in 505 = [Tier-Azure-1, Tier-Azure-2]"
        );
    }

    /**
     * Successful handling of action failure - VM in region.
     */
    @Test
    public void testRegionNewGroupNewPolicy() {
        Action action = createAction(101L, VM_NO_GROUP_2_ID, EntityType.COMPUTE_TIER,
                TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyServiceAccesses(3, 1, 1, 0, 1, 1);
        verifyOutput(
                "Created group vm-in-no-group-2",
                "vm-in-no-group-2 with name 'Region Region-B - Failed execution Tier Family Exclusion Policy (account 123456)'",
                "Create policy response for VM vm-in-no-group-2 = setting_policy"
        );
    }

    /**
     * Successful handling of action failure - VM in region.
     */
    @Test
    public void testAZNewGroupNewPolicy() {
        Action action = createAction(101L, AZ_VM_ID, EntityType.COMPUTE_TIER, TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyServiceAccesses(3, 1, 1, 0, 1, 1);
        verifyOutput(
                "Created group vm-in-az",
                "Creating new family exclusion policy for VM vm-in-az with name 'Availability Zone AvailabilityZone-A",
                "Create policy response for VM vm-in-az = setting_policy"
        );
    }

    /**
     * Initialize Repository test data.
     */
    private void initializeRepositoryService() {
        Long[] requests = {
                AZ_VM_ID,
                GOOD_VM_ID,
                GOOD_VM_B_ID,
                MISSING_ORIGIN_VM_ID,
                MISSING_ACCOUNT_VM_ID,
                VM_NO_GROUP_1_ID,
                VM_NO_GROUP_2_ID,
                OTHER_VM_ID,
                REGION_A_ID,
                REGION_B_ID,
                AZ_A_ID,
                UNKNOWN_REGION_VM_ID,
                AZ_QF_VM_ID
        };
        TopologyEntityDTO[] responses = {
                AZ_VM,
                GOOD_VM,
                GOOD_VM_B,
                MISSING_ORIGIN_VM,
                MISSING_ACCOUNT_VM,
                VM_NO_GROUP_1,
                VM_NO_GROUP_2,
                OTHER_VM,
                REGION_A,
                REGION_B,
                AZ_A,
                UNKNOWN_REGION_VM,
                AZ_QF_VM
        };

        for (int i = 0; i < requests.length; i++) {
            Long request = requests[i];
            TopologyEntityDTO response = responses[i];
            when(testRepositoryService
                    .retrieveTopologyEntities(createRepositoryQuery(response.getEntityType(),
                            request)))
                    .thenReturn(createRepositoryResponse(response));
        }

        // Return multiple entries for the same VM OID
        when(testRepositoryService
                .retrieveTopologyEntities(createRepositoryQuery(EntityType.VIRTUAL_MACHINE_VALUE,
                        DUPLICATE_VM_ID)))
                .thenReturn(createRepositoryResponse(DUPLICATE_VM, DUPLICATE_VM));

        when(testRepositoryService
                .retrieveTopologyEntities(createRepositoryQuery(EntityType.REGION_VALUE, null)))
                .thenReturn(createRepositoryResponse(REGION_A, REGION_B));

        when(testRepositoryService
                .retrieveTopologyEntities(eq(createRepositoryQuery(EntityType.COMPUTE_TIER_VALUE, null))))
                .thenReturn(createRepositoryResponse(COMPUTE_TIERS));
    }

    /**
     * Initialize Group test data.
     */
    private void initializeGroupService() {
        when(testGroupService
                .getGroups(createGroupQuery(GOOD_VM_GROUP_NAME)))
                .thenReturn(createGroupingResponse(GOOD_VM_GROUP_NAME, GOOD_VM_GROUP_ID,
                        GOOD_VM_ID));
        when(testGroupService
                .getGroups(createGroupQuery(GOOD_VM_B_GROUP_NAME)))
                .thenReturn(createGroupingResponse(GOOD_VM_B_GROUP_NAME, GOOD_VM_B_GROUP_ID,
                        GOOD_VM_B_ID));
        GroupDefinition.Builder builder = GroupDefinition.newBuilder()
                .setDisplayName(GOOD_VM_GROUP_NAME)
                .setType(GroupType.REGULAR)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .addMembers(GOOD_VM_ID)));
        when(testGroupService
                .createGroup(createCreateGroupRequest(VM_NO_GROUP_2_NAME, VM_NO_GROUP_2_ID)))
                .thenReturn(CreateGroupResponse.newBuilder()
                        .setGroup(Grouping.newBuilder()
                                .setId(VM_NO_GROUP_2_ID)
                                .addExpectedTypes(MemberType.newBuilder()
                                        .setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                                .setDefinition(builder)
                                .build())
                        .build());
        when(testGroupService
                .createGroup(createCreateGroupRequest(VM_IN_AZ_GROUP_NAME, AZ_VM_ID)))
                .thenReturn(CreateGroupResponse.newBuilder()
                        .setGroup(Grouping.newBuilder()
                                .setId(AZ_VM_GROUP_ID)
                                .addExpectedTypes(MemberType.newBuilder()
                                        .setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                                .setDefinition(builder)
                                .build())
                        .build());
        when(testGroupService
                .createGroup(createCreateGroupRequest(OTHER_GROUP_NAME, OTHER_VM_ID)))
                .thenThrow(new StatusRuntimeException(Status.PERMISSION_DENIED));
    }

    /**
     * Initialize SettingPolicy test data.
     */
    private void initializeSettingPolicyService() {
        // Region Region-A - Failed execution Tier Family Exclusion Policy (account 123456)
        when(testSettingPolicyService
                .getSettingPolicy(createPolicyQuery(REGION_A_POLICY_NAME)))
                .thenReturn(GetSettingPolicyResponse.newBuilder()
                        .setSettingPolicy(createSettingPolicy(REGION_A_POLICY_ID,
                                REGION_A_POLICY_NAME, OTHER_GROUP_ID,
                                OTHER_TIER_ID))
                        .build());
        when(testSettingPolicyService.updateSettingPolicy(any()))
                .thenReturn(UpdateSettingPolicyResponse.newBuilder()
                        .setSettingPolicy(createSettingPolicy(REGION_A_POLICY_ID,
                                REGION_A_POLICY_NAME, OTHER_GROUP_ID,
                                OTHER_TIER_ID))
                        .build());
        SettingPolicyInfo.Builder info = FailedCloudResizeTierExcluder
                .createSettingPolicyInfo(REGION_B_POLICY_NAME,
                        ImmutableList.of(GOOD_VM_B_GROUP_ID), ImmutableList.of(501L, 502L));
        SettingProto.CreateSettingPolicyRequest req = SettingProto.CreateSettingPolicyRequest.newBuilder()
                .setSettingPolicyInfo(info).build();
        when(testSettingPolicyService.createSettingPolicy(any()))
                .thenReturn(CreateSettingPolicyResponse.newBuilder()
                        .setSettingPolicy(createSettingPolicy(301L, REGION_B_POLICY_NAME,
                                299L, 99L))
                        .build());
        when(testSettingPolicyService.createSettingPolicy(eq(req)))
                .thenThrow(new StatusRuntimeException(Status.PERMISSION_DENIED));
    }

    private FailedCloudVMGroupProcessor createFailedCloudVMGroupProcessor(String patterns) {
        return new FailedCloudVMGroupProcessor(groupServiceRpc, repositoryService,
                settingPolicyService,
                Executors.newSingleThreadScheduledExecutor(), 360, patterns);
    }

    private RetrieveTopologyEntitiesRequest createRepositoryQuery(int entityType, Long oid) {
        RetrieveTopologyEntitiesRequest.Builder builder = RetrieveTopologyEntitiesRequest.newBuilder()
                .setReturnType(Type.FULL)
                .addEntityType(entityType);
        if (oid != null) {
            builder.addEntityOids(oid);
        }
        return builder.build();
    }

    private CreateGroupRequest createCreateGroupRequest(String groupName, Long memberOid) {
        return CreateGroupRequest.newBuilder()
                .setGroupDefinition(GroupDefinition.newBuilder()
                        .setDisplayName(groupName)
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .setType(MemberType.newBuilder()
                                                .setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                                        .addMembers(memberOid))))
                .setOrigin(GroupDTO.Origin.newBuilder()
                        .setSystem(System.newBuilder()
                                .setDescription(groupName)))
                .build();
    }

    private GetGroupsRequest createGroupQuery(final String groupName) {
        return GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.REGULAR)
                        .addPropertyFilters(PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                                .setStringFilter(StringFilter.newBuilder()
                                        .setStringPropertyRegex(Pattern.quote(groupName)))))
                .build();
    }

    private GetSettingPolicyRequest createPolicyQuery(final String policyName) {
        return GetSettingPolicyRequest.newBuilder().setName(policyName).build();
    }

    /**
     * Create an entity builder.
     * @param type entity type
     * @param name name of entity
     * @param oid OID of entity
     * @return a builder for a partially-filled in @{link TopologyEntityDTO}
     */
    private TopologyEntityDTO.Builder createEntity(EntityType type, String name, Long oid) {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(type.getNumber())
                .setDisplayName(name)
                .setOid(oid);
    }

    /*
     * Create test compute tiers to be used with the "get tiers" repository request.
     * Family-A: Tier-A1, Tier-A2
     * Family-B: Tier-B1
     */
    private List<TopologyEntityDTO> createComputeTiers() {
        AtomicLong tierId = new AtomicLong(501L);
        List<TopologyEntityDTO> result = Stream.of(
                new Pair<>("Family-A", "Tier-A1"),
                new Pair<>("Family-A", "Tier-A2"),
                new Pair<>("Family-B", "Tier-B1"),
                new Pair<>("Family-Azure-Quota-1", "Tier-Azure-1"),
                new Pair<>("Family-Azure-Quota-2", "Tier-Azure-2"))
                .map(p -> {
                    Long oid = tierId.getAndIncrement();
                    tierNameToOid.put(p.second, oid);
                    ComputeTierInfo.Builder computeTierInfo = ComputeTierInfo.newBuilder().setFamily(p.first);
                    if (p.first.startsWith("Family-Azure-")) {
                        // Azure tier, so add quota family
                        computeTierInfo.setQuotaFamily("Azure-Quota-Family-A");
                    }
                    return TopologyEntityDTO.newBuilder()
                            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                            .setOid(oid)
                            .setDisplayName(p.second)
                            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(computeTierInfo))
                            .build();
                })
                .collect(Collectors.toList());
        UNKNOWN_TIER_ID = tierId.get();
        return result;
    }

    private ActionEntity.Builder createActionEntity(Long id, EntityType entityType) {
        return ActionEntity.newBuilder()
                .setId(id)
                .setType(entityType.getNumber())
                .setEnvironmentType(EnvironmentType.CLOUD);
    }

    private ActionDTO.Action createActionDTO(Long actionId, Long targetId,
            Long destId, EntityType destType) {
        ActionDTO.Action.Builder builder = ActionDTO.Action.newBuilder()
                .setId(actionId)
                .setDeprecatedImportance(0)
                .setSupportingLevel(SupportLevel.SUPPORTED)
                .setExplanation(Explanation.getDefaultInstance());
        Move.Builder move = Move.newBuilder()
                .setTarget(createActionEntity(targetId, EntityType.VIRTUAL_MACHINE));
        if (destType != null) {
            ChangeProvider.Builder cp = ChangeProvider.newBuilder().setDestination(
                    createActionEntity(destId, destType));
            move.addChanges(cp);
            builder.setInfo(ActionInfo.newBuilder().setMove(move));
        } else {
            // Add a different action type
            builder.setInfo(ActionInfo.newBuilder()
                    .setDeactivate(Deactivate.newBuilder()
                            .setTarget(ActionEntity.newBuilder()
                                    .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                    .setId(OTHER_VM_ID))));
        }
        return builder.build();
    }

    private com.vmturbo.action.orchestrator.action.Action createAction(Long actionId, Long vmId,
            EntityType destType, Long destId) {
        ActionDTO.Action actionDTO = createActionDTO(actionId, vmId, destId, destType);
        ActionModeCalculator amc = new ActionModeCalculator();
        com.vmturbo.action.orchestrator.action.Action action =
                new Action(actionDTO, 1L, amc, 1L);
        return action;
    }

    private TopologyEntityDTO createVm(String name, Long oid, boolean addOrigin,
            Long... connectedRegionIdList) {
        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setDisplayName(name);
        if (addOrigin) {
            Map<Long, PerTargetEntityInformation> perTargetEntityInfo = ImmutableMap.of(
                    ACCOUNT_ID, PerTargetEntityInformation.newBuilder()
                            .setVendorId("a0689cde-09b0-4236-a6bb-da80e6ecf9f7")
                            .setOrigin(EntityOrigin.DISCOVERED)
                            .build());
            builder.setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                            .putAllDiscoveredTargetData(perTargetEntityInfo)));
        }
        for (Long connectedRegionId : connectedRegionIdList) {
            // Connect the VM to a region or AZ (if the ID is AZ_A_ID)
            builder.addConnectedEntityList(ConnectedEntity.newBuilder()
                    .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                    .setConnectedEntityId(connectedRegionId)
                    .setConnectedEntityType(
                            connectedRegionId == AZ_A_ID ? EntityType.AVAILABILITY_ZONE_VALUE : EntityType.REGION_VALUE));
        }
        return builder.build();
    }

    private ImmutableList<PartialEntityBatch> createRepositoryResponse(List<TopologyEntityDTO> teList) {
        PartialEntityBatch.Builder builder = PartialEntityBatch.newBuilder();
        teList.stream()
                .map(tier -> PartialEntity.newBuilder().setFullEntity(tier))
                .forEach(builder::addEntities);
        return ImmutableList.of(builder.build());
    }

    private ImmutableList<PartialEntityBatch> createRepositoryResponse(TopologyEntityDTO... teList) {
        return createRepositoryResponse(Arrays.asList(teList));
    }

    private List<Grouping> createGroupingResponse(String groupName, Long groupId, Long memberId) {
        GroupDefinition.Builder builder = GroupDefinition.newBuilder()
                .setDisplayName(groupName)
                .setType(GroupType.REGULAR)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .addMembers(memberId)));
        return ImmutableList.of(Grouping.newBuilder()
                .setId(groupId)
                .addExpectedTypes(MemberType.newBuilder()
                        .setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                .setDefinition(builder)
                .build());
    }

    @Nonnull
    private SettingPolicy createSettingPolicy(long policyId, String policyName, long scopeGroupId, long initialId) {
        return SettingPolicy.newBuilder()
                .setId(policyId)
                .setInfo(FailedCloudResizeTierExcluder.createSettingPolicyInfo(policyName,
                        ImmutableList.of(scopeGroupId),
                        ImmutableList.of(initialId)))
                .build();
    }

    /**
     * Quick and dirty logger appender to capture logging output.
     */
    static class TestOutputAppender implements org.apache.logging.log4j.core.Appender {
        private ErrorHandler errorHandler;
        private List<String> messages;

        TestOutputAppender() {
            this.messages = new ArrayList<>();
        }

        public void clear() {
            this.messages.clear();
        }

        public boolean contains(String string) {
            return this.messages.stream()
                    .anyMatch(msg -> msg.contains(string));
        }

        public List<String> getMessages() {
            return this.messages;
        }

        @Override
        public void append(LogEvent logEvent) {
            if (logEvent != null) {
                this.messages.add(logEvent.getMessage().getFormattedMessage());
            }
        }

        @Override
        public String getName() {
            return "testAppender";
        }

        @Override
        public Layout<? extends Serializable> getLayout() {
            return null;
        }

        @Override
        public boolean ignoreExceptions() {
            return false;
        }

        @Override
        public ErrorHandler getHandler() {
            return this.errorHandler;
        }

        @Override
        public void setHandler(ErrorHandler errorHandler) {
            this.errorHandler = errorHandler;
        }

        @Override
        public State getState() {
            return State.STARTED;
        }

        @Override
        public void initialize() {
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }

        @Override
        public boolean isStarted() {
            return true;
        }

        @Override
        public boolean isStopped() {
            return false;
        }
    }
}

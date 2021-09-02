package com.vmturbo.action.orchestrator.execution;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.core.ErrorHandler;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.stubbing.Answer;

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
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredGroupsPoliciesSettings.UploadedGroup.Builder;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.CreateScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProtoMoles.ScheduleServiceMole;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc.ScheduleServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.DiscoveredGroupInfo;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.GetDiscoveredGroupsResponse;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.TargetDiscoveredGroups;
import com.vmturbo.common.protobuf.topology.DiscoveredGroupMoles.DiscoveredGroupServiceMole;
import com.vmturbo.common.protobuf.topology.DiscoveredGroupServiceGrpc;
import com.vmturbo.common.protobuf.topology.DiscoveredGroupServiceGrpc.DiscoveredGroupServiceBlockingStub;
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
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Tests for logic that handles failed cloud VM resizes.
 */
public class FailedCloudResizePolicyCreatorTest {
    // Constants
    private static final long ACCOUNT_ID = 123456L;
    private static final long MISSING_ORIGIN_VM_ID = 2L;
    private static final long MISSING_ACCOUNT_VM_ID = 3L;
    private static final long AZ_VM_ID = 4L;
    private static final long OTHER_VM_ID = 5L;
    private static final long DUPLICATE_VM_ID = 6L;
    private static final long VM_NO_GROUP_1_ID = 7L;
    private static final long VM_NO_GROUP_2_ID = 8L;
    private static final long MISSING_VM_ID = 9L;
    private static final long AZ_QF_VM_ID = 1L;
    private static final long GOOD_VM_A_ID = 10L;
    private static final long GOOD_VM_B_ID = 11L;
    private static final long GOOD_VM_C_ID = 12L;
    private static final long GOOD_VM_D_ID = 13L;
    private static final long GOOD_VM_E_ID = 14L;
    private static final long GOOD_VM_F_ID = 15L;
    private static final long GOOD_VM_G_ID = 16L;
    private static final long UNKNOWN_REGION_VM_ID = 210L;
    private static final long REGION_A_ID = 401L;
    private static final long REGION_B_ID = 402L;
    private static final long AZ_A_ID = 403L;
    private static final long UNKNOWN_REGION_ID = 499L;

    // Single VM AS
    private static final String AS1_NAME = "AvailabilitySet::as-1";

    // Multi-VM AS
    private static final String AS2_NAME = "AvailabilitySet::as-2";
    private static final Long AS2_ID = 202L;
    private static final String AS3_NAME = "AvailabilitySet::as-3";

    // Cannot create a policy for this AS
    private static final String AS4_NAME = "AvailabilitySet::as-4";

    private static final String GOOD_VM_GROUP_NAME =
            "good-vm/a0689cde-09b0-4236-a6bb-da80e6ecf9f7 - TEST (account 123456)";

    // Discovered group data
    private static final Map<String, Set<Long>> DISCOVERED_GROUPS = ImmutableMap.of(
            AS1_NAME, ImmutableSet.of(GOOD_VM_A_ID),
            AS2_NAME, ImmutableSet.of(GOOD_VM_B_ID, GOOD_VM_C_ID, MISSING_VM_ID, DUPLICATE_VM_ID,
                    MISSING_ACCOUNT_VM_ID),
            AS3_NAME, ImmutableSet.of(GOOD_VM_D_ID, GOOD_VM_E_ID),
            AS4_NAME, ImmutableSet.of(GOOD_VM_F_ID, GOOD_VM_G_ID),
            GOOD_VM_GROUP_NAME, ImmutableSet.of(GOOD_VM_D_ID, GOOD_VM_E_ID)
    );

    // Service initialization
    private final GroupServiceMole testGroupService = spy(new GroupServiceMole());
    private final SettingPolicyServiceMole testSettingPolicyService =
            spy(new SettingPolicyServiceMole());
    private final RepositoryServiceMole testRepositoryService = spy(new RepositoryServiceMole());
    private final DiscoveredGroupServiceMole testDiscoveredGroupService =
            spy(new DiscoveredGroupServiceMole());
    private final ScheduleServiceMole testScheduleService = spy(new ScheduleServiceMole());

    /**
     * gRPC mocking support.
      */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(testGroupService,
            testSettingPolicyService, testRepositoryService, testDiscoveredGroupService,
            testScheduleService);
    private GroupServiceBlockingStub groupServiceRpc;
    private RepositoryServiceBlockingStub repositoryService;
    private SettingPolicyServiceBlockingStub settingPolicyService;
    private DiscoveredGroupServiceBlockingStub discoveredGroupService;
    private FailedCloudVMGroupProcessor failedCloudVMGroupProcessor;
    private ScheduleServiceBlockingStub scheduleService;

        private static final ActionFailure DEFAULT_ACTION_FAILURE = ActionFailure.newBuilder()
                .setErrorDescription("Failure foobar occurred")
                .setActionId(100L).build();
        private static final ActionFailure AZURE_ACTION_FAILURE = ActionFailure.newBuilder()
                .setErrorDescription("Reason: com.microsoft.azure.CloudException: Async operation"
                        + " failed with provisioning state: Failed: Allocation failed. We do not"
                        + " have sufficient capacity for the requested VM size in this region."
                        + " Read more about improving likelihood of allocation success at"
                        + " http://aka.ms/allocation-guidance")
                .setActionId(100L).build();

    private static final TopologyEntityDTO GOOD_VM_A = createVm("good-vm-a", GOOD_VM_A_ID, true, REGION_A_ID);
    private static final TopologyEntityDTO GOOD_VM_B = createVm("good-vm-b", GOOD_VM_B_ID, true, REGION_B_ID);
    private static final TopologyEntityDTO GOOD_VM_C = createVm("good-vm-c", GOOD_VM_C_ID, true, REGION_B_ID);
    private static final TopologyEntityDTO GOOD_VM_D = createVm("good-vm-d", GOOD_VM_D_ID, true, REGION_B_ID);
    private static final TopologyEntityDTO GOOD_VM_E = createVm("good-vm-e", GOOD_VM_E_ID, true, REGION_B_ID);
    private static final TopologyEntityDTO GOOD_VM_F = createVm("good-vm-f", GOOD_VM_F_ID, true, REGION_B_ID);
    private static final TopologyEntityDTO GOOD_VM_G = createVm("good-vm-g", GOOD_VM_G_ID, true, REGION_B_ID);
    private static final TopologyEntityDTO AZ_VM = createVm("vm-in-az", AZ_VM_ID, true, AZ_A_ID);
    private static final TopologyEntityDTO AZ_QF_VM = createVm("vm-in-az", AZ_QF_VM_ID, true, AZ_A_ID);
    private static final TopologyEntityDTO DUPLICATE_VM = createVm("vm-in-az", DUPLICATE_VM_ID, true, AZ_A_ID);
    private static final TopologyEntityDTO VM_NO_GROUP_1 = createVm("vm-in-no-group-1", VM_NO_GROUP_1_ID, true, REGION_A_ID);
    // VM in Region-B.  Region-B has no existing policy defined.
    private static final TopologyEntityDTO VM_NO_GROUP_2 = createVm("vm-in-no-group-2", VM_NO_GROUP_2_ID, true, REGION_B_ID);
    private static final TopologyEntityDTO OTHER_VM = createVm("other-vm", OTHER_VM_ID, true, REGION_B_ID);
    private static final TopologyEntityDTO MISSING_ORIGIN_VM = createVm("no-origin", MISSING_ORIGIN_VM_ID, false);
    private static final TopologyEntityDTO MISSING_ACCOUNT_VM = createVm("no-account", MISSING_ACCOUNT_VM_ID, false);
    private static final TopologyEntityDTO UNKNOWN_REGION_VM = createVm("bad-region", UNKNOWN_REGION_VM_ID,
            true, UNKNOWN_REGION_ID);

    private static final TopologyEntityDTO REGION_A = createEntity(EntityType.REGION, "Region-A", REGION_A_ID).build();
    private static final TopologyEntityDTO REGION_B = createEntity(EntityType.REGION, "Region-B", REGION_B_ID).build();
    private static final TopologyEntityDTO AZ_A = createEntity(EntityType.AVAILABILITY_ZONE,
            "AvailabilityZone-A", AZ_A_ID).build();
    private static final Map<String, Long> tierNameToOid = new HashMap<>();
    private static final List<TopologyEntityDTO> COMPUTE_TIERS = createComputeTiers();
    private static final Long TIER_A1_OID = tierNameToOid.get("Tier-A1");
    private static final Long TIER_A2_OID = tierNameToOid.get("Tier-A2");

    TestOutputAppender output = new TestOutputAppender();

    /**
     * Initialization common to all tests.
     */
    @Before
    public void setUp() {
        // Add a logging appender so that I can capture logging output
        ((org.apache.logging.log4j.core.Logger)FailedCloudResizePolicyCreator.logger)
                .addAppender(output);
        ((org.apache.logging.log4j.core.Logger)FailedCloudResizePolicyCreator.logger).setLevel(
                org.apache.logging.log4j.Level.DEBUG);
        output.clear();
        groupServiceRpc = GroupServiceGrpc.newBlockingStub(testServer.getChannel());
        repositoryService =
                RepositoryServiceGrpc.newBlockingStub(testServer.getChannel());
        settingPolicyService = SettingPolicyServiceGrpc
                .newBlockingStub(testServer.getChannel());
        discoveredGroupService = DiscoveredGroupServiceGrpc.newBlockingStub(testServer.getChannel());
        scheduleService =
                com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc.newBlockingStub(testServer.getChannel());
        initializeRepositoryService();
        initializeGroupService();
        initializeDiscoveredGroupService();
        initializeSettingPolicyService();
        initializeScheduleService();
        String failedActionPatterns = "Action failed\nFailure .* occurred\nThe cluster where the"
                + " VM/Availability Set is allocated is currently out of capacity\ncom\\.micro"
                + "soft\\.azure\\.CloudException: Async operation failed with provisioning state:"
                + " Failed: Allocation failed";
        failedCloudVMGroupProcessor = createFailedCloudVMGroupProcessor(failedActionPatterns);
    }

    private void initializeScheduleService() {
        when(testScheduleService.createSchedule(any()))
                .thenReturn(CreateScheduleResponse.newBuilder()
                        .setSchedule(Schedule.newBuilder().setId(1L))
                        .build());
    }

    /**
     * Remove logging interceptor.
     */
    @After
    public void tearDown() {
        ((org.apache.logging.log4j.core.Logger)FailedCloudResizePolicyCreator.logger)
                .removeAppender(output);
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
     * Verify that the test output does not contain a string.
     *
     * @param strings patterns to locate in the test output.
     */
    private void verifyNoOutput(String... strings) {
        Set<String> patterns = new HashSet<>(Arrays.asList(strings));
        output.getMessages().stream()
                .filter(patterns::contains)
                .findFirst()
                .ifPresent(msg -> Assert.fail("Not expecting in output: " + msg));
        }

    /**
     * Verify that invalid regular expressions are handled properly.
     */
    @Test
    public void testFailedPatternParsing() {
        String badFailedActionPatterns = "Action failed\nFailure (.* occurred";
        FailedCloudVMGroupProcessor gp = createFailedCloudVMGroupProcessor(badFailedActionPatterns);
        List<Pattern> patterns = gp.getFailedCloudResizePolicyCreator().getFailedActionPatterns();
        Assert.assertEquals(1, patterns.size());
        Assert.assertEquals("Action failed", patterns.get(0).pattern());
    }

    /**
     * Verify that valid regular expressions are handled properly.
     */
    @Test
    public void testSuccessfulPatternParsing() {
        List<Pattern> patterns = failedCloudVMGroupProcessor
                .getFailedCloudResizePolicyCreator().getFailedActionPatterns();
        Assert.assertEquals(4, patterns.size());
    }

    /**
     * Verify that poorly formed actions are handled correctly.
     */
    @Test
    public void invalidActionFormat() {
        // Null action
        Action action = createAction(101L, GOOD_VM_A_ID, EntityType.COMPUTE_TIER, TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, null);
        verifyOutput("Not creating recommend only policy for action 101");
    }

    /**
     * Missing move information in action.
     */
    @Test
    public void missingMove() {
        // Create an action with no move.
        Action action = createAction(101L, GOOD_VM_A_ID, null, null);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyOutput("Not creating recommend only policy for action 101: Missing move/scale info");
    }

    /**
     * Verify that successful actions are not processed.
     */
    @Test
    public void testSuccessfulAction() {
        Action action = createAction(101L, GOOD_VM_A_ID, EntityType.COMPUTE_TIER, TIER_A1_OID);
        // Action was successful
        ActionFailure actionFailure = ActionFailure.newBuilder()
                .setErrorDescription("Action succeeded")
                .setActionId(100L).build();
        failedCloudVMGroupProcessor.handleActionFailure(action, actionFailure);
        verifyOutput("Not creating recommend only policy for action 101: Action is successful");
    }

    /**
     * Verify that failed actions are processed.
     */
    @Test
    public void failedActionNoAS() {
        Action action = createAction(101L, GOOD_VM_A_ID, EntityType.COMPUTE_TIER, TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyNoOutput("Creating recommend only policy for AvailabilitySet::as-1 due to failed action ID 101");
    }

    /**
     * Ensure that we do not create a recommend only policy for single VM availability sets.
     */
    @Test
    public void failedActionSingleVMAS() {
        Action action = createAction(101L, GOOD_VM_A_ID, EntityType.COMPUTE_TIER, TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyNoOutput("Creating recommend only policy for AvailabilitySet::as-1 due to failed action ID 101");
    }

    /**
     * Test the sunny day scenario.
     */
    @Test
    public void failedActionMultiVMAS() {
        Action action = createAction(101L, GOOD_VM_B_ID, EntityType.COMPUTE_TIER, TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyOutput("Creating recommend only policy for AvailabilitySet::as-2 due to failed action ID 101");
    }

    /**
     * Ensure that we handle creating a policy when the policy exists.
     */
    @Test
    public void failedActionMultiVMASUpdateExistingPolicy() {
        // VM D belongs to AS-3, which already has a policy.
        Action action = createAction(101L, GOOD_VM_D_ID, EntityType.COMPUTE_TIER, TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyOutput("Policy 'AvailabilitySet::as-3 - Failed execution recommend only Policy (account 123456)' exists - updating expiration time");
    }

    /**
     * Ensure that we handle policy creation failures.
     */
    @Test
    public void policyCreationError() {
        // VM G belongs to AS-4. The test environment will fail policy creation for this.
        Action action = createAction(101L, GOOD_VM_G_ID, EntityType.COMPUTE_TIER, TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyOutput("Cannot create/update policy 'AvailabilitySet::as-4 - Failed execution recommend only Policy (account 123456)");
    }

    /**
     * Ensures that a VM is present and has all required fields.
     */
    @Test
    public void testMissingVm() {
        // VM doesn't exist
        Action action = createAction(101L, MISSING_VM_ID, EntityType.COMPUTE_TIER, TIER_A2_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
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
    }

    /**
     * Cannot locate account ID in VM.
     */
    @Test
    public void testMissingAccountVm() {
        // The VM is malformed, so handleActionFailure will return before accessing any services
        Action action = createAction(102L, MISSING_ACCOUNT_VM_ID, EntityType.COMPUTE_TIER, TIER_A2_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, DEFAULT_ACTION_FAILURE);
        verifyOutput("Not creating recommend only policy for action 102: Cannot find account ID");
    }

    /**
     * Ensure that the known Azure error message from OM-64464 is detected as a failed action.
     */
    @Test
    public void testAzureActionFailure() {
        Action action = createAction(101L, GOOD_VM_B_ID, EntityType.COMPUTE_TIER,
                TIER_A1_OID);
        failedCloudVMGroupProcessor.handleActionFailure(action, AZURE_ACTION_FAILURE);
        verifyOutput("Creating recommend only policy for AvailabilitySet::as-2 due to failed action ID 101");
    }

    /**
     * Initialize Repository test data.
     */
    private void initializeRepositoryService() {
        Long[] requests = {
                AZ_VM_ID,
                GOOD_VM_A_ID,
                GOOD_VM_B_ID,
                GOOD_VM_C_ID,
                GOOD_VM_D_ID,
                GOOD_VM_E_ID,
                GOOD_VM_F_ID,
                GOOD_VM_G_ID,
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
                GOOD_VM_A,
                GOOD_VM_B,
                GOOD_VM_C,
                GOOD_VM_D,
                GOOD_VM_E,
                GOOD_VM_F,
                GOOD_VM_G,
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
        // Add discovered groups
        for (Entry<String, Set<Long>> entry : DISCOVERED_GROUPS.entrySet()) {
            when(testGroupService
                    .getGroups(createGroupQuery(entry.getKey())))
                    .thenReturn(createGroupingResponse(entry.getKey(), AS2_ID, entry.getValue()));
        }
    }

    /**
     * Initialize discovered group test data.
     */
    private void initializeDiscoveredGroupService() {
        TargetDiscoveredGroups.Builder groups = TargetDiscoveredGroups.newBuilder();
        for (Entry<String, Set<Long>> entry : DISCOVERED_GROUPS.entrySet()) {
            groups.addGroup(DiscoveredGroupInfo.newBuilder()
                    .setUploadedGroup(makeUploadedGroup(entry.getKey(), entry.getValue())));
        }
        when(testDiscoveredGroupService
                .getDiscoveredGroups(any()))
                .thenReturn(GetDiscoveredGroupsResponse.newBuilder()
                        .putGroupsByTargetId(1L, groups.build())
                        .build());
    }

    private Builder makeUploadedGroup(String groupName, Set<Long> oidList) {
        return UploadedGroup.newBuilder()
                .setDefinition(GroupDefinition.newBuilder()
                        .setDisplayName(groupName)
                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                .addMembersByType(StaticMembersByType.newBuilder()
                                        .addAllMembers(oidList)
                                        .setType(MemberType.newBuilder()
                                                .setEntity(EntityType.VIRTUAL_MACHINE_VALUE)))));
    }

    /**
     * Initialize SettingPolicy test data.
     */
    private void initializeSettingPolicyService() {
        // Pre-populate a recommend only policy for duplicate policy testing.
        long expiration = java.lang.System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1L);
        String policyName = "AvailabilitySet::as-3 - Failed execution recommend only Policy (account 123456)";
        when(testSettingPolicyService.listSettingPolicies(any()))
                .thenReturn(ImmutableList.of(
                        SettingProto.SettingPolicy.newBuilder()
                                .setInfo(SettingPolicyInfo.newBuilder()
                                        .setName(StringConstants.AVAILABILITY_SET_RECOMMEND_ONLY_PREFIX
                                                + expiration + ":" + policyName)
                                        .setDisplayName(policyName)
                                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                                .build()));

        // Add a create request that will fail
        when(testSettingPolicyService.createSettingPolicy(any()))
                .thenAnswer((Answer<CreateSettingPolicyResponse>)invocation -> {
                    CreateSettingPolicyResponse.Builder builder =
                            CreateSettingPolicyResponse.newBuilder();
                    CreateSettingPolicyRequest req =
                            invocation.getArgumentAt(0, CreateSettingPolicyRequest.class);
                    String policyName1 = req.getSettingPolicyInfo().getDisplayName();
                    if (!policyName1.contains("AvailabilitySet::as-4")) {
                        builder.setSettingPolicy(SettingPolicy.newBuilder().setId(301L));
                    }
                    return builder.build();
                });
    }

    private FailedCloudVMGroupProcessor createFailedCloudVMGroupProcessor(String patterns) {
        return new FailedCloudVMGroupProcessor(groupServiceRpc, repositoryService,
                settingPolicyService, scheduleService,
                Executors.newSingleThreadScheduledExecutor(), discoveredGroupService,
                360, patterns, 1L);
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

    private GetGroupsRequest createGroupQuery(final String groupName) {
        return GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                        .setGroupType(GroupType.REGULAR)
                        .addPropertyFilters(PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                                .setStringFilter(StringFilter.newBuilder()
                                        .setStringPropertyRegex(SearchProtoUtil
                                                        .escapeSpecialCharactersInLiteral(groupName)))))
                .build();
    }

    /**
     * Create an entity builder.
     * @param type entity type
     * @param name name of entity
     * @param oid OID of entity
     * @return a builder for a partially-filled in @{link TopologyEntityDTO}
     */
    private static TopologyEntityDTO.Builder createEntity(EntityType type, String name, Long oid) {
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
    private static List<TopologyEntityDTO> createComputeTiers() {
        AtomicLong tierId = new AtomicLong(501L);
        return Stream.of(
                new Pair<>("Family-A", "Tier-A1"),
                new Pair<>("Family-A", "Tier-A2"),
                new Pair<>("Family-B", "Tier-B1"),
                new Pair<>("Family-Azure-Quota-1", "Tier-Azure-1"),
                new Pair<>("Family-Azure-Quota-2", "Tier-Azure-2"))
                .map(p -> {
                    long oid = tierId.getAndIncrement();
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
        return new Action(actionDTO, 1L, amc, 1L);
    }

    private static TopologyEntityDTO createVm(String name, Long oid, boolean addOrigin,
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

    private List<Grouping> createGroupingResponse(String groupName, Long groupId,
            Set<Long> memberIds) {
        GroupDefinition.Builder builder = GroupDefinition.newBuilder()
                .setDisplayName(groupName)
                .setType(GroupType.REGULAR)
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .addAllMembers(memberIds)));
        return ImmutableList.of(Grouping.newBuilder()
                .setId(groupId)
                .addExpectedTypes(MemberType.newBuilder()
                        .setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                .setDefinition(builder)
                .build());
    }

    /**
     * Quick and dirty logger appender to capture logging output.
     */
    static class TestOutputAppender implements org.apache.logging.log4j.core.Appender {
        private ErrorHandler errorHandler;
        private final List<String> messages;

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

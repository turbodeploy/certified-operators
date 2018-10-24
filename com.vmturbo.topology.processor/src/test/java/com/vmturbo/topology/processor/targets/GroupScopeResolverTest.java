package com.vmturbo.topology.processor.targets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse.Members;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.GroupScopeProperty;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.GroupScopePropertySet;
import com.vmturbo.platform.sdk.common.EntityPropertyName;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;

/**
 * Test the functionality of the the class GroupScopeResolver.
 */
public class GroupScopeResolverTest {

    private Long realtimeTopologyContextId = 777777L;

    private static long[] groupId = {1L, 11L};

    private static long[] memberId = {2L, 22L};

    private static String[] displayName = {"VM1", "VM2"};

    private static double[] VCPU_CAPACITY = {2000.0, 200.0};

    private static double[] VMEM_CAPACITY = {4000.0, 400.0};

    private static double[] BALOONING_CAPACITY = {6000.0, 600.0};

    private static String[] VSTORAGE_KEY = {"FooBar_Foo_Bar", "NewKey_fubar"};

    private static String[] VSTORAGE_PREFIX = {"FooBar_Foo_", "NewKey_"};

    private static String[] IP_ADDRESS = {"10.10.150.140", "10.10.150.125"};

    private static AccountDefEntry addressAccountDefEntry = AccountDefEntry.newBuilder()
            .setCustomDefinition(
                    CustomAccountDefEntry.newBuilder()
                            .setName(PredefinedAccountDefinition.Address.name().toLowerCase())
                            .setDisplayName("this is my address")
                            .setDescription("The address")
                            .setIsSecret(false))
            .setMandatory(true)
            .build();

    private static AccountDefEntry groupScopeAccountDefEntry = AccountDefEntry.newBuilder()
            .setCustomDefinition(
                    CustomAccountDefEntry.newBuilder()
                            .setName(PredefinedAccountDefinition.ScopedVms.name().toLowerCase())
                            .setDisplayName("Scope to VMs")
                            .setDescription("A scope containing all supported VM properties")
                            .setIsSecret(false)
                            .setGroupScope(GroupScopePropertySet.newBuilder()
                                    .setEntityType(EntityType.VIRTUAL_MACHINE)
                                    .addProperty(GroupScopeProperty.newBuilder()
                                            .setPropertyName(EntityPropertyName.DISPLAY_NAME.name())
                                            .setIsMandatory(true))
                                    .addProperty(GroupScopeProperty.newBuilder()
                                            .setPropertyName(EntityPropertyName.STATE.name())
                                            .setIsMandatory(true))
                                    .addProperty(GroupScopeProperty.newBuilder()
                                            .setPropertyName(EntityPropertyName.UUID.name())
                                            .setIsMandatory(true))
                                    .addProperty(GroupScopeProperty.newBuilder()
                                            .setPropertyName(EntityPropertyName.IP_ADDRESS.name())
                                            .setIsMandatory(true))
                                    .addProperty(GroupScopeProperty.newBuilder()
                                            .setPropertyName(EntityPropertyName.VMEM_CAPACITY.name())
                                            .setIsMandatory(true))
                                    .addProperty(GroupScopeProperty.newBuilder()
                                            .setPropertyName(EntityPropertyName.VCPU_CAPACITY.name())
                                            .setIsMandatory(true))
                                    .addProperty(GroupScopeProperty.newBuilder()
                                            .setPropertyName(EntityPropertyName.MEM_BALLOONING.name())
                                            .setIsMandatory(true))
                                    .addProperty(GroupScopeProperty.newBuilder()
                                            .setPropertyName(EntityPropertyName.VSTORAGE_KEY_PREFIX
                                                    .name())
                                            .setIsMandatory(true)))
                            .setIsSecret(false))
            .setMandatory(true)
            .build();

    private static AccountDefEntry groupScopeMissingMandatory =
            addGroupScopeProperty(groupScopeAccountDefEntry,
                    EntityPropertyName.GUEST_LOAD_UUID.name(),
                    true);

    private static AccountDefEntry groupScopeMissingNonMandatory =
            addGroupScopeProperty(groupScopeAccountDefEntry,
                    EntityPropertyName.GUEST_LOAD_UUID.name(),
                    false);

    private static AccountValue addressAccountVal = createAccountValue(
            PredefinedAccountDefinition.Address.name().toLowerCase(),
            "1313 Mockingbird Lane");

    private static AccountValue getGroupScopeAccountVal(int index) {
        return createAccountValue(
                PredefinedAccountDefinition.ScopedVms.name().toLowerCase(),
                Long.toString(groupId[index]));
    }

    private final static GetGroupResponse getGetGroupResponse(int index) {
        return GetGroupResponse.newBuilder()
                .setGroup(Group.newBuilder()
                        .setId(groupId[index])
                        .setGroup(GroupInfo.newBuilder()
                                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                        .build())
                .build();
    }

    private final static GetMembersResponse getGetMembersResponse(int index) {
        return GetMembersResponse.newBuilder()
                .setMembers(Members.newBuilder()
                        .addIds(memberId[index])
                        .build())
                .build();
    }

    private final static RetrieveTopologyEntitiesResponse getRetrieveEntitiesResponse(int index) {
        return RetrieveTopologyEntitiesResponse.newBuilder()
                .addEntities(TopologyEntityDTO.newBuilder()
                        .setOid(memberId[index])
                        .setDisplayName(displayName[index])
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                        .setType(CommodityType.VCPU_VALUE))
                                .setCapacity(VCPU_CAPACITY[index])
                                .build())
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                        .setType(CommodityType.VMEM_VALUE))
                                .setCapacity(VMEM_CAPACITY[index])
                                .build())
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                        .setType(CommodityType.BALLOONING_VALUE))
                                .setCapacity(BALOONING_CAPACITY[index])
                                .build())
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                        .setType(CommodityType.VSTORAGE_VALUE)
                                        .setKey(VSTORAGE_KEY[index]))
                                .build())
                        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                        .addIpAddresses(IpAddress.newBuilder()
                                                .setIpAddress(IP_ADDRESS[index])
                                                .build())
                                        .build())
                                .build())
                        .setEntityState(EntityState.MAINTENANCE)
                        .build())
                .build();
    }

    private GroupScopeResolver groupScopeResolver;

    @Rule
    public GrpcTestServer groupServer = GrpcTestServer.newServer(new TestGroupService());

    @Rule
    public GrpcTestServer repositoryServer = GrpcTestServer.newServer(new TestRepositoryService());

    @Before
    public void setup() throws Exception {
        groupScopeResolver = new GroupScopeResolver(groupServer.getChannel(),
                repositoryServer.getChannel(),
                realtimeTopologyContextId);
    }

    @Test
    public void testBasicAccountValues() throws Exception {

        ProbeInfo pi = ProbeInfo.newBuilder()
                .setProbeCategory("test")
                .setProbeType("vc")
                .addTargetIdentifierField(PredefinedAccountDefinition.Address.name().toLowerCase())
                .addAccountDefinition(addressAccountDefEntry)
                .build();

        Collection<AccountValue> retVal = groupScopeResolver
                .processGroupScope(Collections.singletonList(addressAccountVal),
                        pi.getAccountDefinitionList());
        assertEquals(addressAccountVal, retVal.iterator().next());
    }

    @Test
    public void testGroupScope() throws Exception {
        acctDefEntryTester(groupScopeAccountDefEntry, true, 0);
    }

    @Test
    public void testChangingGroupMembership() throws Exception {
        acctDefEntryTester(groupScopeAccountDefEntry, true, 1, 0);
        acctDefEntryTester(groupScopeAccountDefEntry, true, 1, 1);
    }
    @Test
    public void testGroupScopeMissingMandatoryValue() throws Exception {
        acctDefEntryTester(groupScopeMissingMandatory, false, 0);
    }

    @Test
    public void testGroupScopeMissingOptionalValue() throws Exception {
        acctDefEntryTester(groupScopeMissingNonMandatory, true, 0);
    }

    private void acctDefEntryTester(AccountDefEntry groupScopeAcctDefToTest, boolean expectSuccess,
                                    int indexForGroupScopeAccountDef)
            throws Exception {
        acctDefEntryTester(groupScopeAcctDefToTest, expectSuccess, indexForGroupScopeAccountDef,
                indexForGroupScopeAccountDef);
    }

    /**
     * Test that a groupScopeAcctDef populates the account value properly.
     *
     * @param groupScopeAcctDefToTest The group scope account definition.
     * @param expectSuccess Indicates whether we expect the account values to be properly populated.
     *                      We have some failure cases that we test and set this to false.
     * @param indexForGroupScopeAccountDef Indicates which of the two account definitions we are
     *                                     using for the test.
     * @param indexForAccountValue Indicates which account values we expect to be returned.  For the
     *                             case where we test changing group membership, we actually expect
     *                             to get different values in some cases than the index of the group
     *                             scope account def would indicate.
     * @throws Exception
     */
    private void acctDefEntryTester(AccountDefEntry groupScopeAcctDefToTest, boolean expectSuccess,
                                    int indexForGroupScopeAccountDef, int indexForAccountValue)
            throws Exception {
        ProbeInfo pi = ProbeInfo.newBuilder()
                .setProbeCategory("test")
                .setProbeType("vc")
                .addTargetIdentifierField(PredefinedAccountDefinition.Address.name().toLowerCase())
                .addAccountDefinition(addressAccountDefEntry)
                .addAccountDefinition(groupScopeAcctDefToTest)
                .build();

        Collection<AccountValue> retVal = groupScopeResolver
                .processGroupScope(ImmutableList.of(addressAccountVal,
                        getGroupScopeAccountVal(indexForGroupScopeAccountDef)),
                        pi.getAccountDefinitionList());
        assertEquals(2, retVal.size());
        assertTrue(retVal.contains(addressAccountVal));
        Optional<AccountValue> groupScope = retVal.stream()
                .filter(acctVal -> acctVal.getKey()
                        .equalsIgnoreCase(PredefinedAccountDefinition.ScopedVms.name()))
                .findFirst();
        assertTrue(groupScope.isPresent());
        if (!expectSuccess) {
            assertEquals(0, groupScope.get().getGroupScopePropertyValuesCount());
            return;
        }
        assertEquals(8,
                groupScope.get().getGroupScopePropertyValues(0).getValueCount());
        assertEquals(displayName[indexForAccountValue], groupScope.get().getGroupScopePropertyValues(0).getValue(0));
        assertEquals(EntityState.MAINTENANCE.toString(), groupScope.get()
                .getGroupScopePropertyValues(0).getValue(1));
        assertEquals(Long.toString(memberId[indexForAccountValue]), groupScope.get().getGroupScopePropertyValues(0)
                .getValue(2));
        assertEquals(IP_ADDRESS[indexForAccountValue], groupScope.get().getGroupScopePropertyValues(0).getValue(3));
        assertEquals(VMEM_CAPACITY[indexForAccountValue], Double.parseDouble(groupScope.get()
                .getGroupScopePropertyValues(0).getValue(4)), 0.1);
        assertEquals(VCPU_CAPACITY[indexForAccountValue], Double.parseDouble(groupScope.get()
                .getGroupScopePropertyValues(0).getValue(5)), 0.1);
        assertEquals(BALOONING_CAPACITY[indexForAccountValue], Double.parseDouble(groupScope.get()
                .getGroupScopePropertyValues(0).getValue(6)), 0.1);
        assertEquals(VSTORAGE_PREFIX[indexForAccountValue], groupScope.get().getGroupScopePropertyValues(0)
                .getValue(7));
    }

    private static AccountValue createAccountValue(@Nonnull String key, @Nonnull String value) {
        return AccountValue.newBuilder().setKey(key)
                .setStringValue(value)
                .build();
    }

    private static AccountDefEntry addGroupScopeProperty(AccountDefEntry accountDef,
                                                  String property,
                                                  boolean isMandatory) {
        AccountDefEntry.Builder builder = AccountDefEntry.newBuilder(accountDef);
        builder.getCustomDefinitionBuilder()
                .getGroupScopeBuilder()
                .addProperty(GroupScopeProperty.newBuilder()
                        .setPropertyName(property)
                        .setIsMandatory(isMandatory))
                .build();
        return builder.build();
    }

    public class TestGroupService extends GroupServiceImplBase {
        /**
         * We use this variable to alternate group membership for the group with the second group ID.
         * Its membership will flip flop with each call to getMembers.
         */
        private int indexForChangingGroupMembership = 0;

        @Override
        public void getGroup(final GroupID request, final StreamObserver<GetGroupResponse> responseObserver) {
            // figure out which response to use based on which groupId is in the request
            int index = 0;
            if (request.getId() == (groupId[1])) {
                index = 1;
            }
            responseObserver.onNext(getGetGroupResponse(index));
            responseObserver.onCompleted();
        }

        @Override
        public void getMembers(final GetMembersRequest request, final StreamObserver<GetMembersResponse> responseObserver) {
            // figure out which response to use based on which groupId is in the request
            int index = 0;
            // if the request is for the second group, alternate the membership with each
            // call.  This is how we emulate changing group membership.
            if (request.getId() == (groupId[1])) {
                index = indexForChangingGroupMembership++ % 2;
            }
            responseObserver.onNext(getGetMembersResponse(index));
            responseObserver.onCompleted();
        }
    }

    public class TestRepositoryService extends RepositoryServiceImplBase {
        @Override
        public void retrieveTopologyEntities(final RetrieveTopologyEntitiesRequest request,
                                             final StreamObserver<RetrieveTopologyEntitiesResponse>
                                                     responseObserver) {
            int index = 0;
            if (request.getEntityOids(0) == memberId[1]) {
                index = 1;
            }
            responseObserver.onNext(getRetrieveEntitiesResponse(index));
            responseObserver.onCompleted();
        }
    }
}

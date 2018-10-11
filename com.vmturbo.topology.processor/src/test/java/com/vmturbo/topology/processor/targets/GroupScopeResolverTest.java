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
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.GroupScopeProperty;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.GroupScopePropertySet;
import com.vmturbo.platform.sdk.common.EntityPropertyName;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;

/**
 * Test the functionality of the the class GroupScopeResolver.
 */
public class GroupScopeResolverTest {

    private Long realtimeTopologyContextId = 777777L;

    private static long groupId = 1L;

    private static long memberId = 2L;

    private static String displayName = "VM1";

    private static double VCPU_CAPACITY = 2000.0;

    private static double VMEM_CAPACITY = 4000.0;

    private static double BALOONING_CAPACITY = 6000.0;

    private static String VSTORAGE_KEY = "FooBar_Foo_Bar";

    private static String VSTORAGE_PREFIX = "FooBar_Foo_";

    private static String IP_ADDRESS = "10.10.150.140";

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

    private static AccountValue groupScopeAccountVal = createAccountValue(
            PredefinedAccountDefinition.ScopedVms.name().toLowerCase(),
            Long.toString(groupId));private GroupServiceBlockingStub groupService;

    private final static GetGroupResponse GET_GROUP_RESPONSE = GetGroupResponse.newBuilder()
            .setGroup(Group.newBuilder()
                    .setId(groupId)
                    .setGroup(GroupInfo.newBuilder()
                            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE))
                    .build())
            .build();

    private final static GetMembersResponse GET_MEMBERS_RESPONSE = GetMembersResponse.newBuilder()
            .setMembers(Members.newBuilder()
                    .addIds(memberId)
                    .build())
            .build();

    private final static RetrieveTopologyEntitiesResponse RETRIEVE_ENTITIES_RESPONSE =
            RetrieveTopologyEntitiesResponse.newBuilder()
            .addEntities(TopologyEntityDTO.newBuilder()
                    .setOid(memberId)
                    .setDisplayName(displayName)
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                    .setType(CommodityType.VCPU_VALUE))
                            .setCapacity(VCPU_CAPACITY)
                            .build())
                    .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                    .setType(CommodityType.VMEM_VALUE))
                            .setCapacity(VMEM_CAPACITY)
                            .build())
                    .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                    .setType(CommodityType.BALLOONING_VALUE))
                            .setCapacity(BALOONING_CAPACITY)
                            .build())
                    .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                            .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                                    .setType(CommodityType.VSTORAGE_VALUE)
                            .setKey(VSTORAGE_KEY))
                            .build())
                    .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                            .setVirtualMachine(VirtualMachineInfo.newBuilder()
                                    .addIpAddresses(IpAddress.newBuilder()
                                            .setIpAddress(IP_ADDRESS)
                                            .build())
                                    .build())
                            .build())
                    .setEntityState(EntityState.MAINTENANCE)
                    .build())
            .build();

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
                .processGroupScope(Collections.singletonList(addressAccountVal), pi);
        assertEquals(addressAccountVal, retVal.iterator().next());
    }

    @Test
    public void testGroupScope() throws Exception {
        acctDefEntryTester(groupScopeAccountDefEntry, true);
    }

    @Test
    public void testGroupScopeMissingMandatoryValue() throws Exception {
        acctDefEntryTester(groupScopeMissingMandatory, false);
    }

    @Test
    public void testGroupScopeMissingOptionalValue() throws Exception {
        acctDefEntryTester(groupScopeAccountDefEntry, true);
    }

    private void acctDefEntryTester(AccountDefEntry groupScopeAcctDefToTest, boolean expectSuccess)
            throws Exception {
        ProbeInfo pi = ProbeInfo.newBuilder()
                .setProbeCategory("test")
                .setProbeType("vc")
                .addTargetIdentifierField(PredefinedAccountDefinition.Address.name().toLowerCase())
                .addAccountDefinition(addressAccountDefEntry)
                .addAccountDefinition(groupScopeAcctDefToTest)
                .build();

        Collection<AccountValue> retVal = groupScopeResolver
                .processGroupScope(ImmutableList.of(addressAccountVal, groupScopeAccountVal), pi);
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
        assertEquals(displayName, groupScope.get().getGroupScopePropertyValues(0).getValue(0));
        assertEquals(EntityState.MAINTENANCE.toString(), groupScope.get()
                .getGroupScopePropertyValues(0).getValue(1));
        assertEquals(Long.toString(memberId), groupScope.get().getGroupScopePropertyValues(0)
                .getValue(2));
        assertEquals(IP_ADDRESS, groupScope.get().getGroupScopePropertyValues(0).getValue(3));
        assertEquals(VMEM_CAPACITY, Double.parseDouble(groupScope.get()
                .getGroupScopePropertyValues(0).getValue(4)), 0.1);
        assertEquals(VCPU_CAPACITY, Double.parseDouble(groupScope.get()
                .getGroupScopePropertyValues(0).getValue(5)), 0.1);
        assertEquals(BALOONING_CAPACITY, Double.parseDouble(groupScope.get()
                .getGroupScopePropertyValues(0).getValue(6)), 0.1);
        assertEquals(VSTORAGE_PREFIX, groupScope.get().getGroupScopePropertyValues(0)
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
        @Override
        public void getGroup(final GroupID request, final StreamObserver<GetGroupResponse> responseObserver) {
            responseObserver.onNext(GET_GROUP_RESPONSE);
            responseObserver.onCompleted();
        }

        @Override
        public void getMembers(final GetMembersRequest request, final StreamObserver<GetMembersResponse> responseObserver) {
            responseObserver.onNext(GET_MEMBERS_RESPONSE);
            responseObserver.onCompleted();
        }
    }

    public class TestRepositoryService extends RepositoryServiceImplBase {
        @Override
        public void retrieveTopologyEntities(final RetrieveTopologyEntitiesRequest request, final StreamObserver<RetrieveTopologyEntitiesResponse> responseObserver) {
            responseObserver.onNext(RETRIEVE_ENTITIES_RESPONSE);
            responseObserver.onCompleted();
        }
    }
}

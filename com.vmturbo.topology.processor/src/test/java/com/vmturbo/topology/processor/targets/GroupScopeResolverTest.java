package com.vmturbo.topology.processor.targets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsResponse;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceImplBase;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.GroupScopeProperty;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.GroupScopePropertySet;
import com.vmturbo.platform.sdk.common.EntityPropertyName;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.PredefinedAccountDefinition;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.topology.processor.entity.EntityNotFoundException;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 * Test the functionality of the the class GroupScopeResolver.
 */
public class GroupScopeResolverTest {

    // 111 is corresponding to an empty group
    private static long[] groupId = {1L, 11L, 111L};

    private static long[] memberId = {2L, 22L};

    private static long[] guestLoadId = {12345L, 12346L};

    private static long[] targetId = {233L, 234L};

    private static String[] displayName = {"VM1", "VM2"};

    private static String[] guestLoadDisplayName = {"App1", "App2"};

    private static double[] VCPU_CAPACITY = {2000.0, 200.0};

    private static double[] VMEM_CAPACITY = {4000.0, 400.0};

    private static String[] VSTORAGE_KEY = {"FooBar_Foo_Bar", "NewKey_fubar"};

    private static String[] VSTORAGE_PREFIX = {
        "_wK4GWWTbEd-Ea97W1fNhs6\\foo.eng.vmturbo.com\\vm-2",
        "_wK4GWWTbEd-Ea97W1fNhs6\\foo.eng.vmturbo.com\\vm-22"
    };

    private static String[] IP_ADDRESS = {"10.10.150.140", "10.10.150.125"};

    private static final String SCOPE_ACCOUNT_VALUE_NAME = "businessAccountBySubscriptionIdScope";

    private static final long[] BUSINESS_ACCOUNT_OID = {333L, 3333L};

    private static final String[] SUBSCRIPTION_ID = {"ABCD", "EFGH"};

    private static final String OFFER_ID = "offerId123";

    private static final String ENROLLMENT_NUMBER = "enrollment123";

    private static SDKProbeType validProbeType = SDKProbeType.AWS;

    private static ProbeCategory validProbeCategory = ProbeCategory.CLOUD_MANAGEMENT;

    private static SDKProbeType invalidProbeType = SDKProbeType.SNMP;

    private static ProbeCategory invalidProbeCategory = ProbeCategory.GUEST_OS_PROCESSES;

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
                        .setPropertyName(EntityPropertyName.GUEST_LOAD_UUID.name())
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
                        .setPropertyName(EntityPropertyName.VSTORAGE_KEY_PREFIX
                            .name())
                        .setIsMandatory(true)))
                .setIsSecret(false))
        .setMandatory(true)
        .build();

    private static AccountDefEntry entityScopeAccountDefEntry = AccountDefEntry.newBuilder()
        .setCustomDefinition(
            CustomAccountDefEntry.newBuilder()
                .setName(SCOPE_ACCOUNT_VALUE_NAME)
                .setDisplayName("Scope to BusinessAccount by Subscription ID")
                .setDescription("A scope containing all supported BusinessAccount properties")
                .setIsSecret(false)
                .setEntityScope(GroupScopePropertySet.newBuilder()
                    .setEntityType(EntityType.BUSINESS_ACCOUNT)
                    .addProperty(GroupScopeProperty.newBuilder()
                        .setPropertyName(EntityPropertyName.OFFER_ID.name())
                        .setIsMandatory(true))
                    .addProperty(GroupScopeProperty.newBuilder()
                        .setPropertyName(EntityPropertyName.ENROLLMENT_NUMBER.name())
                        .setIsMandatory(true)))
                .setIsSecret(false))
        .setMandatory(true)
        .build();

    private static AccountDefEntry groupScopeMissingMandatory =
            addGroupScopeProperty(groupScopeAccountDefEntry,
                    EntityPropertyName.MEM_BALLOONING.name(),
                    true);

    private static AccountDefEntry groupScopeMissingNonMandatory =
            addGroupScopeProperty(groupScopeAccountDefEntry,
                    EntityPropertyName.MEM_BALLOONING.name(),
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
                .setGroup(Grouping.newBuilder()
                        .setId(groupId[index])
                        .addExpectedTypes(MemberType.newBuilder()
                                        .setEntity(EntityType.VIRTUAL_MACHINE_VALUE))
                        .setDefinition(GroupDefinition.newBuilder()
                                )
                        .build())
                .build();
    }

    private final static GetMembersResponse getGetMembersResponse(long groupId, int index) {
        return GetMembersResponse.newBuilder()
            .setGroupId(groupId)
            .addMemberId(memberId[index])
            .build();
    }

    private final static PartialEntityBatch getRetrieveScopedVMEntitiesResponse(int index) {
        return PartialEntityBatch.newBuilder()
            .addEntities(PartialEntity.newBuilder()
                .setFullEntity(TopologyEntityDTO.newBuilder()
                    .setOid(memberId[index])
                    .setDisplayName(displayName[index])
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setOrigin(Origin.newBuilder()
                            .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                    .putDiscoveredTargetData(targetId[index],
                                        PerTargetEntityInformation.getDefaultInstance())))
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
                    .setEntityState(EntityState.MAINTENANCE)))
            .build();
    }

    private final static PartialEntityBatch getRetrieveGuestLoadAppEntitiesResponse(int index) {
        return PartialEntityBatch.newBuilder()
                .addEntities(PartialEntity.newBuilder()
                    .setFullEntity(TopologyEntityDTO.newBuilder()
                        .setOid(guestLoadId[index])
                        .setDisplayName(guestLoadDisplayName[index])
                        .setEntityType(EntityType.APPLICATION_VALUE)
                        .setOrigin(Origin.newBuilder()
                                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                        .putDiscoveredTargetData(targetId[index],
                                            PerTargetEntityInformation.getDefaultInstance())))
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                                .newBuilder().setProviderId(memberId[index]))))
                .build();
    }

    private static PartialEntityBatch getRetrieveScopeBusinessAccountResponse(String subscriptionId) {
        PartialEntityBatch.Builder builder = PartialEntityBatch.newBuilder();
        if (subscriptionId == SUBSCRIPTION_ID[1]) {
            builder.addEntities(PartialEntity.newBuilder()
                .setFullEntity(TopologyEntityDTO.newBuilder()
                    .setOrigin(Origin.newBuilder()
                        .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                            .putDiscoveredTargetData(targetId[0],
                                PerTargetEntityInformation.getDefaultInstance())))
                    .setOid(BUSINESS_ACCOUNT_OID[1])
                    .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                    .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setBusinessAccount(BusinessAccountInfo.newBuilder()
                            .setAccountId(subscriptionId)))));
        } else if (subscriptionId == SUBSCRIPTION_ID[0]) {
            builder.addEntities(PartialEntity.newBuilder()
                .setFullEntity(TopologyEntityDTO.newBuilder()
                    .setOrigin(Origin.newBuilder()
                        .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                            .putDiscoveredTargetData(targetId[0],
                                PerTargetEntityInformation.getDefaultInstance())))
                    .setOid(BUSINESS_ACCOUNT_OID[0])
                    .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                    .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setBusinessAccount(BusinessAccountInfo.newBuilder()
                            .setAccountId(subscriptionId)
                            .addAllPricingIdentifiers(Arrays.asList(
                                PricingIdentifier.newBuilder()
                                    .setIdentifierName(PricingIdentifierName.OFFER_ID)
                                    .setIdentifierValue(OFFER_ID)
                                    .build(),
                                PricingIdentifier.newBuilder()
                                    .setIdentifierName(PricingIdentifierName.ENROLLMENT_NUMBER)
                                    .setIdentifierValue(ENROLLMENT_NUMBER)
                                    .build()))))));
        }
        return builder.build();
    }

    private GroupScopeResolver groupScopeResolver;

    @Rule
    public GrpcTestServer groupServer = GrpcTestServer.newServer(new TestGroupService());

    @Rule
    public GrpcTestServer repositoryServer = GrpcTestServer.newServer(new TestSearchService());

    private TargetStore targetStore = Mockito.mock(TargetStore.class);

    private EntityStore entityStore = Mockito.mock(EntityStore.class);

    private static final String TARGET_ADDRESS = "foo.eng.vmturbo.com";

    @Before
    public void setup() throws Exception {
        groupScopeResolver = new GroupScopeResolver(groupServer.getChannel(),
                repositoryServer.getChannel(), targetStore, entityStore);
        Mockito.when(targetStore.getProbeTypeForTarget(Mockito.anyLong()))
                .thenReturn(Optional.of(validProbeType));
        Mockito.when(targetStore.getProbeCategoryForTarget(Mockito.anyLong()))
                .thenReturn(Optional.of(validProbeCategory));
        Mockito.when(targetStore.getTargetDisplayName(Mockito.anyLong())).thenReturn(Optional.of(TARGET_ADDRESS));
        Mockito.when(entityStore.chooseEntityDTO(2)).thenReturn(EntityDTO.newBuilder()
            .setId("fakeId1")
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .addEntityProperties(EntityProperty.newBuilder()
                .setNamespace(SDKUtil.DEFAULT_NAMESPACE)
                .setName(SupplyChainConstants.LOCAL_NAME)
                .setValue("vm-2"))
            .build());
        Mockito.when(entityStore.chooseEntityDTO(22)).thenReturn(EntityDTO.newBuilder()
            .setId("fakeId2")
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .addEntityProperties(EntityProperty.newBuilder()
                .setNamespace(SDKUtil.DEFAULT_NAMESPACE)
                .setName(SupplyChainConstants.LOCAL_NAME)
                .setValue("vm-22"))
            .build());
        Mockito.when(entityStore.chooseEntityDTO(BUSINESS_ACCOUNT_OID[0])).thenReturn(EntityDTO.newBuilder()
            .setId("fakeId3")
            .setEntityType(EntityType.BUSINESS_ACCOUNT)
            .addEntityProperties(EntityProperty.newBuilder()
                .setNamespace(SDKUtil.DEFAULT_NAMESPACE)
                .setName(SupplyChainConstants.LOCAL_NAME)
                .setValue("ba-00"))
            .build());
        Mockito.when(entityStore.chooseEntityDTO(BUSINESS_ACCOUNT_OID[1])).thenReturn(EntityDTO.newBuilder()
            .setId("fakeId4")
            .setEntityType(EntityType.BUSINESS_ACCOUNT)
            .addEntityProperties(EntityProperty.newBuilder()
                .setNamespace(SDKUtil.DEFAULT_NAMESPACE)
                .setName(SupplyChainConstants.LOCAL_NAME)
                .setValue("ba-01"))
            .build());
    }

    @Test
    public void testBasicAccountValues() throws Exception {

        ProbeInfo pi = ProbeInfo.newBuilder()
                .setProbeCategory("test")
                .setUiProbeCategory("test")
                .setProbeType("VCENTER")
                .addTargetIdentifierField(PredefinedAccountDefinition.Address.name().toLowerCase())
                .addAccountDefinition(addressAccountDefEntry)
                .build();
        Collection<AccountValue> retVal = groupScopeResolver
                .processGroupScope(SDKProbeType.VCENTER,
                    Collections.singletonList(addressAccountVal),
                    pi.getAccountDefinitionList());
        assertEquals(addressAccountVal, retVal.iterator().next());
    }

    @Test
    public void testGroupScope() throws Exception {
        acctDefEntryTester(groupScopeMissingNonMandatory, true, 0);
    }

    @Test
    public void testChangingGroupMembership() throws Exception {
        acctDefEntryTester(groupScopeMissingNonMandatory, true, 1, 0);
        acctDefEntryTester(groupScopeMissingNonMandatory, true, 1, 1);
    }
    @Test
    public void testGroupScopeMissingMandatoryValue() throws Exception {
        acctDefEntryTester(groupScopeMissingMandatory, false, 0);
    }

    @Test
    public void testGroupScopeMissingOptionalValue() throws Exception {
        acctDefEntryTester(groupScopeMissingNonMandatory, true, 0);
    }

    @Test
    public void testGroupScopeInvalidProbeType() throws Exception {
        Mockito.when(targetStore.getProbeTypeForTarget(Mockito.anyLong()))
                .thenReturn(Optional.of(invalidProbeType));
        Mockito.when(targetStore.getProbeCategoryForTarget(Mockito.anyLong()))
                .thenReturn(Optional.of(invalidProbeCategory));
        acctDefEntryTester(groupScopeMissingNonMandatory, false, 0);
    }

    @Test
    public void testEmptyGroupScope() {
        ProbeInfo pi = ProbeInfo.newBuilder()
            .setProbeCategory("testCategory")
            .setUiProbeCategory("testCategory")
            .setProbeType("testProbeType")
            .addAccountDefinition(groupScopeAccountDefEntry)
            .build();
        List<AccountValue> retVal = groupScopeResolver
            .processGroupScope(SDKProbeType.VCENTER,
                ImmutableList.of(getGroupScopeAccountVal(2)),
                pi.getAccountDefinitionList());
        assertEquals(1, retVal.size());
        assertEquals(PredefinedAccountDefinition.ScopedVms.name().toLowerCase(), retVal.get(0).getKey());
        // check that no exception is thrown when processing empty scope and property values count
        // after processing is 0
        assertEquals(0, retVal.get(0).getGroupScopePropertyValuesCount());
    }

    /**
     * Make sure that if and entity is deleted from the entity store, but still in the repository,
     * we fail gracefully.
     *
     * @throws Exception should never happen.
     */
    @Test
    public void testGroupScopeEntityDeleted() throws Exception {
        Mockito.when(entityStore.chooseEntityDTO(2))
            .thenThrow(new EntityNotFoundException("Entity Not Found."));
            acctDefEntryTester(groupScopeMissingNonMandatory, false, 0);
    }

    /**
     * Test an entity scope where the subscription ID matches a BusinessAccount that has OfferID
     * and enrollment number fields populated.
     */
    @Test
    public void testSuccessfulEntityScope() {
        entityScopeAcctDefEntryTester(entityScopeAccountDefEntry, true,
            SUBSCRIPTION_ID[0]);
    }

    /**
     * Test an entity scope where the subscription ID matches an existing BusinessAccount, but
     * that BusinessAccount has no values populated for OfferID and enrollment number.
     */
    @Test
    public void testEmptyEntityScope() {
        entityScopeAcctDefEntryTester(entityScopeAccountDefEntry, false,
            SUBSCRIPTION_ID[1]);
    }

    /**
     * Test an entity scope where no BusinessAccount exists with a matching subscription ID.
     */
    @Test
    public void testNonmatchingSubscriptionIdEntityScope() {
        entityScopeAcctDefEntryTester(entityScopeAccountDefEntry, false,
            "missing");
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
                .setUiProbeCategory("test")
                .setProbeType("VCENTER")
                .addTargetIdentifierField(PredefinedAccountDefinition.Address.name().toLowerCase())
                .addAccountDefinition(addressAccountDefEntry)
                .addAccountDefinition(groupScopeAcctDefToTest)
                .build();

        Collection<AccountValue> retVal = groupScopeResolver
                .processGroupScope(SDKProbeType.VCENTER,
                    ImmutableList.of(addressAccountVal,
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
        assertEquals(9,
                groupScope.get().getGroupScopePropertyValues(0).getValueCount());
        assertEquals(displayName[indexForAccountValue], groupScope.get().getGroupScopePropertyValues(0).getValue(0));
        assertEquals(EntityState.MAINTENANCE.toString(), groupScope.get()
                .getGroupScopePropertyValues(0).getValue(1));
        assertEquals(Long.toString(memberId[indexForAccountValue]), groupScope.get().getGroupScopePropertyValues(0)
                .getValue(2));
        assertEquals(Long.toString(guestLoadId[indexForAccountValue]), groupScope.get().getGroupScopePropertyValues(0)
                .getValue(3));
        assertEquals(IP_ADDRESS[indexForAccountValue], groupScope.get().getGroupScopePropertyValues(0).getValue(4));
        assertEquals(VMEM_CAPACITY[indexForAccountValue], Double.parseDouble(groupScope.get()
                .getGroupScopePropertyValues(0).getValue(5)), 0.1);
        assertEquals(VCPU_CAPACITY[indexForAccountValue], Double.parseDouble(groupScope.get()
                .getGroupScopePropertyValues(0).getValue(6)), 0.1);
        assertEquals(VSTORAGE_PREFIX[indexForAccountValue], groupScope.get().getGroupScopePropertyValues(0)
                .getValue(7));
    }

    private void entityScopeAcctDefEntryTester(AccountDefEntry entityScopeAcctDefToTest,
                                               boolean expectSuccess,
                                     String subscriptionId) {
        ProbeInfo pi = ProbeInfo.newBuilder()
            .setProbeCategory("test")
            .setUiProbeCategory("test")
            .setProbeType(SDKProbeType.AZURE.getProbeType())
            .addTargetIdentifierField(PredefinedAccountDefinition.Address.name().toLowerCase())
            .addAccountDefinition(addressAccountDefEntry)
            .addAccountDefinition(entityScopeAcctDefToTest)
            .build();
        Collection<AccountValue> retVal = groupScopeResolver
            .processGroupScope(SDKProbeType.AZURE,
                ImmutableList.of(addressAccountVal,
                    createAccountValue(SCOPE_ACCOUNT_VALUE_NAME,
                        subscriptionId)),
                pi.getAccountDefinitionList());
        assertEquals(2, retVal.size());
        assertTrue(retVal.contains(addressAccountVal));
        Optional<AccountValue> entityScope = retVal.stream()
            .filter(acctVal -> acctVal.getKey()
                .equalsIgnoreCase(SCOPE_ACCOUNT_VALUE_NAME))
            .findFirst();
        assertTrue(entityScope.isPresent());
        if (!expectSuccess) {
            assertEquals(0, entityScope.get().getGroupScopePropertyValuesCount());
            return;
        }
        assertEquals(2,
            entityScope.get().getGroupScopePropertyValues(0).getValueCount());
        assertEquals(OFFER_ID, entityScope.get().getGroupScopePropertyValues(0).getValue(0));
        assertEquals(ENROLLMENT_NUMBER,
            entityScope.get().getGroupScopePropertyValues(0).getValue(1));
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
            int index = request.getId() == (groupId[1]) ? 1 : 0;
            responseObserver.onNext(getGetGroupResponse(index));
            responseObserver.onCompleted();
        }

        @Override
        public void getMembers(final GetMembersRequest request, final StreamObserver<GetMembersResponse> responseObserver) {
            // return empty group for groupId[2]
            if (request.getIdList().contains(groupId[2])) {
                responseObserver.onNext(
                        GetMembersResponse.newBuilder().setGroupId(groupId[2]).build());
            }

            // figure out which response to use based on which groupId is in the request
            int index = 0;
            // if the request is for the second group, alternate the membership with each
            // call.  This is how we emulate changing group membership.
            if (request.getIdList().contains(groupId[1])) {
                index = indexForChangingGroupMembership++ % 2;
                responseObserver.onNext(getGetMembersResponse(groupId[1], index));
            } else {
                responseObserver.onNext(getGetMembersResponse(groupId[0], index));
            }
            responseObserver.onCompleted();
        }
    }

    public class TestSearchService extends SearchServiceImplBase {

        @Override
        public void searchEntitiesStream(final SearchEntitiesRequest request,
                                   final StreamObserver<PartialEntityBatch> responseObserver) {
            long requestedOid;
            boolean guestLoad = false;
            // if there is an entity oid in the request, use it to make an entity request
            if (request.getEntityOidCount() > 0) {
                requestedOid = request.getEntityOid(0);
            } else {
                // if there is no entity in the request, it means it is a guestLoad request
                // the starting filter contains the OID
                requestedOid = Long.parseLong(request.getSearch().getSearchParametersList().iterator().next()
                    .getStartingFilter().getStringFilter().getOptions(0));
                guestLoad = true;
            }
            responseObserver.onNext(retrieveSearchEntities(requestedOid, guestLoad));
            responseObserver.onCompleted();
        }

        @Override
        public void searchEntityOids(final SearchEntityOidsRequest request,
                                     final StreamObserver<SearchEntityOidsResponse> responseObserver) {
            responseObserver.onNext(retrieveSearchEntitiesOids(request));
            responseObserver.onCompleted();
        }

        @Nonnull
        private PartialEntityBatch retrieveSearchEntities(long oid, boolean retrieveGuestLoad) {
            PartialEntityBatch response = PartialEntityBatch.getDefaultInstance();
            for (int j = 0; j < BUSINESS_ACCOUNT_OID.length; j++) {
                if (BUSINESS_ACCOUNT_OID[j] == oid) {
                    response = getRetrieveScopeBusinessAccountResponse(SUBSCRIPTION_ID[j]);
                }
            }
            for (int i = 0; i < memberId.length; i ++) {
                if (memberId[i] == oid) {
                    response = retrieveGuestLoad
                        ? getRetrieveGuestLoadAppEntitiesResponse(i)
                        : getRetrieveScopedVMEntitiesResponse(i);
                }
            }
            return response;
        }

        @Nonnull
        private SearchEntityOidsResponse retrieveSearchEntitiesOids(SearchEntityOidsRequest request) {
            // Make sure this is a properly formatted request for one of the business account whose
            // subscription ID we know about.  If so, return its OID.  If not, return an empty
            // response.
            SearchEntityOidsResponse response = SearchEntityOidsResponse.getDefaultInstance();
            if (request.getSearch().getSearchParametersCount() == 1) {
                SearchParameters searchParams = request.getSearch().getSearchParameters(0);
                if (searchParams.hasStartingFilter()) {
                    PropertyFilter startingPropFilter = searchParams.getStartingFilter();
                    if ("entityType".equals(startingPropFilter.getPropertyName())
                        && startingPropFilter.hasNumericFilter()
                        && startingPropFilter.getNumericFilter().getValue()
                        == EntityType.BUSINESS_ACCOUNT_VALUE) {
                        if (searchParams.getSearchFilterCount() == 1) {
                            SearchFilter searchFilter = searchParams.getSearchFilter(0);
                            if (searchFilter.hasPropertyFilter() &&
                                SearchableProperties.BUSINESS_ACCOUNT_INFO_REPO_DTO_PROPERTY_NAME
                                    .equals(searchFilter.getPropertyFilter().getPropertyName())) {
                                PropertyFilter propFilter = searchFilter.getPropertyFilter();
                                if (propFilter.hasObjectFilter() &&
                                    propFilter.getObjectFilter().getFiltersCount() == 1) {
                                    PropertyFilter accountFilter =
                                        propFilter.getObjectFilter().getFilters(0);
                                    if (SearchableProperties.BUSINESS_ACCOUNT_INFO_ACCOUNT_ID
                                        .equals(accountFilter.getPropertyName())) {
                                        if (accountFilter.hasStringFilter() &&
                                            accountFilter.getStringFilter().getOptionsCount() == 1) {
                                            String subscriptionId =
                                                accountFilter.getStringFilter().getOptions(0);
                                            for (int i = 0; i < SUBSCRIPTION_ID.length; i++) {
                                                if (SUBSCRIPTION_ID[i].equals(subscriptionId)) {
                                                    response = SearchEntityOidsResponse.newBuilder()
                                                        .addEntities(BUSINESS_ACCOUNT_OID[i])
                                                        .build();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return response;
        }
    }
}

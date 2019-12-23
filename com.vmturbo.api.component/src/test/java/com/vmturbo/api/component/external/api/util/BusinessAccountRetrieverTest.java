package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.EntityFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever.BusinessAccountMapper;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever.SupplementaryData;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever.SupplementaryDataFactory;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.group.BillingFamilyApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest.AccountExpenseQueryScope;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest.AccountExpenseQueryScope.IdList;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesResponse;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTO.CountGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Unit tests for {@link BusinessAccountRetriever}.
 */
public class BusinessAccountRetrieverTest {
    private static final long ENTITY_OID = 7L;

    private static final TopologyEntityDTO ACCOUNT = TopologyEntityDTO.newBuilder()
        .setOid(ENTITY_OID)
        .setDisplayName("foo")
        .setEntityType(UIEntityType.BUSINESS_ACCOUNT.typeNumber())
        .build();

    private ThinTargetCache thinTargetCache = mock(ThinTargetCache.class);

    private CostServiceMole costBackend = spy(CostServiceMole.class);

    private GroupServiceMole groupServiceMole = spy(GroupServiceMole.class);

    /**
     * gRPC server to mock out any calls to other components.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(costBackend, groupServiceMole);

    private RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private BusinessAccountMapper mockMapper = mock(BusinessAccountMapper.class);

    private GroupExpander mockGroupExpander = mock(GroupExpander.class);

    private EntityFilterMapper entityFilterMapper;

    private SearchFilterResolver searchFilterResolver;

    private BusinessAccountRetriever businessAccountRetriever;

    /**
     * Sets up the tests.
     */
    @Before
    public void init() {
        final GroupUseCaseParser groupUseCaseParser =
                new GroupUseCaseParser("groupBuilderUsecases.json");
        this.entityFilterMapper = new EntityFilterMapper(groupUseCaseParser);
        searchFilterResolver = Mockito.mock(SearchFilterResolver.class);
        Mockito.when(searchFilterResolver.resolveExternalFilters(Mockito.any()))
                .thenAnswer(invocation -> invocation.getArguments()[0]);
        businessAccountRetriever =
                new BusinessAccountRetriever(repositoryApi, mockGroupExpander, thinTargetCache,
                        entityFilterMapper, mockMapper, searchFilterResolver);
    }

    /**
     * Test getting all business accounts via
     * {@link BusinessAccountRetriever#getBusinessAccountsInScope(List, List)}.
     */
    @Test
    public void testGetBusinessAccountsInScopeNoTargets() {
        // ARRANGE
        final RepositoryApi.SearchRequest mockReq = ApiTestUtils.mockSearchFullReq(Collections.singletonList(ACCOUNT));
        when(repositoryApi.newSearchRequestMulti(any()))
            .thenReturn(mockReq);

        final BusinessUnitApiDTO convertedDto = mock(BusinessUnitApiDTO.class);
        when(mockMapper.convert(any(), anyBoolean()))
            .thenReturn(Collections.singletonList(convertedDto));

        // ACT
        final List<BusinessUnitApiDTO> results = businessAccountRetriever.getBusinessAccountsInScope(null, null);

        // ASSERT
        verify(repositoryApi).newSearchRequestMulti(Collections.singletonList(
                SearchProtoUtil.makeSearchParameters(
                        SearchProtoUtil.entityTypeFilter(UIEntityType.BUSINESS_ACCOUNT)).build()));
        verify(mockMapper).convert(Collections.singletonList(ACCOUNT), true);

        assertThat(results, is(Collections.singletonList(convertedDto)));
    }

    /**
     * Test getting all business accounts when there are none in the system.
     */
    @Test
    public void testGetBusinessAccountsNone() {
        // ARRANGE
        final RepositoryApi.SearchRequest mockReq = ApiTestUtils.mockEmptySearchReq();
        when(repositoryApi.newSearchRequestMulti(any()))
            .thenReturn(mockReq);

        // ACT
        final List<BusinessUnitApiDTO> results = businessAccountRetriever.getBusinessAccountsInScope(null, null);

        // ASSERT
        assertThat(results, is(Collections.emptyList()));
    }

    /**
     * Test getting all business accounts discovered by a particular target.
     */
    @Test
    public void testGetBusinessAccountsInTargetScope() {
        // ARRANGE
        final long targetId = 321;
        final long nonTargetId = 4321;

        // Mock the search request to return the matching account
        final RepositoryApi.SearchRequest mockReq =
            ApiTestUtils.mockSearchFullReq(Collections.singletonList(ACCOUNT));
        when(repositoryApi.newSearchRequestMulti(any()))
            .thenReturn(mockReq);

        // Mock the mapper to convert the account to an API DTO.
        final BusinessUnitApiDTO convertedDto = mock(BusinessUnitApiDTO.class);
        when(mockMapper.convert(any(), anyBoolean()))
            .thenReturn(Collections.singletonList(convertedDto));

        when(thinTargetCache.getTargetInfo(targetId)).thenReturn(Optional.of(mock(ThinTargetInfo.class)));
        when(thinTargetCache.getTargetInfo(nonTargetId)).thenReturn(Optional.empty());

        // ACT
        final List<BusinessUnitApiDTO> results = businessAccountRetriever.getBusinessAccountsInScope(
            Lists.newArrayList(Long.toString(targetId), Long.toString(nonTargetId)), null);

        // ASSERT
        Mockito.verify(repositoryApi)
                .newSearchRequestMulti(Collections.singletonList(
                        SearchProtoUtil.makeSearchParameters(
                                SearchProtoUtil.entityTypeFilter(UIEntityType.BUSINESS_ACCOUNT))
                                .addSearchFilter(SearchProtoUtil.searchFilterProperty(
                                        SearchProtoUtil.discoveredBy(targetId)))
                                .build()));
        // "allAccounts" false, because we're scoped to a target.
        verify(mockMapper).convert(Collections.singletonList(ACCOUNT), false);

        assertThat(results, is(Collections.singletonList(convertedDto)));
    }

    /**
     * Test getting the business account that belongs to a valid group.
     */
    @Test
    public void testGetBusinessAccountsInScopeOfGroup() {
        // ARRANGE
        final String groupId = "123";
        final Set<Long> expandedOids = ImmutableSet.of(ENTITY_OID);

        // Mock the search request to return the matching accounts
        final RepositoryApi.SearchRequest mockReq =
            ApiTestUtils.mockSearchFullReq(Lists.newArrayList(ACCOUNT));
        when(repositoryApi.newSearchRequestMulti(Mockito.anyCollectionOf(SearchParameters.class)))
            .thenReturn(mockReq);

        when(thinTargetCache.getTargetInfo(Long.parseLong(groupId))).thenReturn(Optional.empty());
        when(mockGroupExpander.getGroup(groupId)).thenReturn(Optional.of(Grouping.newBuilder()
                .setId(Long.parseLong(groupId))
                .setDefinition(GroupDefinition.newBuilder().setType(GroupType.REGULAR)
                    .setDisplayName("group_of_accounts").setIsTemporary(true))
                .build()
            ));
        when(mockGroupExpander.expandUuid(groupId)).thenReturn(expandedOids);

        // Mock the mapper to convert the account to an API DTO.
        final BusinessUnitApiDTO convertedDto = mock(BusinessUnitApiDTO.class);
        when(mockMapper.convert(any(), anyBoolean()))
            .thenReturn(Collections.singletonList(convertedDto));

        // ACT
        final List<BusinessUnitApiDTO> results = businessAccountRetriever.getBusinessAccountsInScope(
            Lists.newArrayList(groupId), null);

        // ASSERT
        verify(repositoryApi).newSearchRequestMulti(Collections.singletonList(
                SearchProtoUtil.makeSearchParameters(
                        SearchProtoUtil.entityTypeFilter(UIEntityType.BUSINESS_ACCOUNT))
                        .addSearchFilter(SearchProtoUtil.searchFilterProperty(
                                SearchProtoUtil.idFilter(expandedOids)))
                        .build()));

        verify(mockMapper).convert(Lists.newArrayList(ACCOUNT), false);

        assertThat(results, is(Collections.singletonList(convertedDto)));
    }

    /**
     * Test getting a single account by OID.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGetBusinessAccountByOid() throws Exception {
        // ARRANGE
        final RepositoryApi.SearchRequest mockReq = ApiTestUtils.mockSearchFullReq(Collections.singletonList(ACCOUNT));
        when(repositoryApi.newSearchRequest(any()))
            .thenReturn(mockReq);

        final BusinessUnitApiDTO convertedDto = mock(BusinessUnitApiDTO.class);
        when(mockMapper.convert(any(), anyBoolean()))
            .thenReturn(Collections.singletonList(convertedDto));

        // ACT
        final BusinessUnitApiDTO result = businessAccountRetriever.getBusinessAccount(Long.toString(ENTITY_OID));

        // ASSERT
        verify(repositoryApi).newSearchRequest(SearchProtoUtil.makeSearchParameters(
            SearchProtoUtil.idFilter(ENTITY_OID))
                .addSearchFilter(SearchProtoUtil.searchFilterProperty(
                    SearchProtoUtil.entityTypeFilter(UIEntityType.BUSINESS_ACCOUNT)))
                .build());
        verify(mockMapper).convert(Collections.singletonList(ACCOUNT), false);

        assertThat(result, is(convertedDto));
    }

    /**
     * Test that getting an account with a badly formatted ID throws an exception.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test(expected = InvalidOperationException.class)
    public void testGetBusinessAccountByOidInvalidArgument() throws Exception {
        businessAccountRetriever.getBusinessAccount("I AM GROOT");
    }

    /**
     * Test that getting a non-existing account throws an exception.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test(expected = UnknownObjectException.class)
    public void testGetBusinessAccountByOidNotFound() throws Exception {
        RepositoryApi.SearchRequest mockReq = ApiTestUtils.mockEmptySearchReq();
        when(repositoryApi.newSearchRequest(any()))
            .thenReturn(mockReq);

        businessAccountRetriever.getBusinessAccount(Long.toString(ENTITY_OID));
    }

    /**
     * Test getting a single account by OID.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGetBillingFamilies() {
        // ARRANGE
        final TopologyEntityDTO parentAccount = TopologyEntityDTO.newBuilder()
            .setOid(ENTITY_OID + 1)
            .setDisplayName("bar")
            .setEntityType(UIEntityType.BUSINESS_ACCOUNT.typeNumber())
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityType(UIEntityType.BUSINESS_ACCOUNT.typeNumber())
                .setConnectedEntityId(ENTITY_OID)
                .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

        // Set up the search request to return the parent and child accounts
        RepositoryApi.SearchRequest mockReq = ApiTestUtils.mockSearchFullReq(
            Arrays.asList(ACCOUNT, parentAccount));
        when(repositoryApi.newSearchRequestMulti(any()))
            .thenReturn(mockReq);

        // Set up the converted account DTOs. We need them to put together the billing
        // family API DTO.
        // If this gets more complex we should split the "mapping" part into a helper class and
        // mock.
        final BusinessUnitApiDTO convertedChild = new BusinessUnitApiDTO();
        convertedChild.setUuid(Long.toString(ENTITY_OID));
        convertedChild.setDisplayName(ACCOUNT.getDisplayName());
        convertedChild.setCostPrice(2.0f);

        final BusinessUnitApiDTO convertedParent = new BusinessUnitApiDTO();
        convertedParent.setMaster(true);
        convertedParent.setDisplayName(parentAccount.getDisplayName());
        convertedParent.setChildrenBusinessUnits(Collections.singleton(convertedChild.getUuid()));
        convertedParent.setUuid(Long.toString(parentAccount.getOid()));
        convertedParent.setCostPrice(1.0f);

        when(mockMapper.convert(any(), anyBoolean()))
            .thenReturn(Arrays.asList(convertedChild, convertedParent));

        // ACT
        final List<BillingFamilyApiDTO> billingFamilies = businessAccountRetriever.getBillingFamilies();

        // ASSERT
        assertThat(billingFamilies.size(), is(1));
        final BillingFamilyApiDTO billingFamily = billingFamilies.get(0);

        assertThat(billingFamily.getDisplayName(), is(convertedParent.getDisplayName()));
        assertThat(billingFamily.getClassName(), is(StringConstants.BILLING_FAMILY));
        // One member. Doesn't count the master account.
        assertThat(billingFamily.getMembersCount(), is(1));
        assertThat(billingFamily.getMasterAccountUuid(), is(convertedParent.getUuid()));
        assertThat(billingFamily.getUuidToNameMap(), is(ImmutableMap.of(
            convertedParent.getUuid(), convertedParent.getDisplayName(),
            convertedChild.getUuid(), convertedChild.getDisplayName())));
        assertThat(billingFamily.getCostPrice(), is(convertedParent.getCostPrice() + convertedChild.getCostPrice()));
        assertThat(billingFamily.getEnvironmentType(), is(EnvironmentType.CLOUD));

    }

    /**
     * Test getting a single account by OID.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGetChildAccounts() throws Exception {
        final EntityWithConnections parentAccount = EntityWithConnections.newBuilder()
            .setOid(ENTITY_OID + 1)
            .setDisplayName("bar")
            .setEntityType(UIEntityType.BUSINESS_ACCOUNT.typeNumber())
            // Owns a VM, and a child account.
            .addConnectedEntities(ConnectedEntity.newBuilder()
                .setConnectedEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                .setConnectedEntityId(121212)
                .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .addConnectedEntities(ConnectedEntity.newBuilder()
                .setConnectedEntityType(UIEntityType.BUSINESS_ACCOUNT.typeNumber())
                .setConnectedEntityId(ENTITY_OID)
                .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

        final RepositoryApi.SingleEntityRequest parentReq = ApiTestUtils.mockSingleEntityRequest(parentAccount);
        when(repositoryApi.entityRequest(anyLong())).thenReturn(parentReq);

        RepositoryApi.SearchRequest mockReq = ApiTestUtils.mockSearchFullReq(Collections.singletonList(ACCOUNT));
        when(repositoryApi.newSearchRequest(any()))
            .thenReturn(mockReq);

        final BusinessUnitApiDTO convertedDto = mock(BusinessUnitApiDTO.class);
        when(mockMapper.convert(any(), anyBoolean()))
            .thenReturn(Collections.singletonList(convertedDto));

        // ACT
        final List<BusinessUnitApiDTO> results = businessAccountRetriever.getChildAccounts(Long.toString(parentAccount.getOid()));

        // ASSERT
        verify(repositoryApi).entityRequest(parentAccount.getOid());
        verify(repositoryApi).newSearchRequest(
            SearchProtoUtil.makeSearchParameters(SearchProtoUtil.idFilter(ENTITY_OID))
                .addSearchFilter(SearchProtoUtil.searchFilterProperty(
                    SearchProtoUtil.entityTypeFilter(UIEntityType.BUSINESS_ACCOUNT)))
                .build());
        verify(mockMapper).convert(Collections.singletonList(ACCOUNT), false);

        assertThat(results, is(Collections.singletonList(convertedDto)));
    }

    /**
     * Test that getting child accounts throws an exception when the input UUID is naughty.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test(expected = InvalidOperationException.class)
    public void getChildAccountsInvalidId() throws Exception {
        businessAccountRetriever.getChildAccounts("I AM NOT AN ID");
    }

    /**
     * Test the cost price retrieval of {@link SupplementaryDataFactory}, when given specific
     * accounts ids.
     */
    @Test
    public void testSupplementaryDataFactorySpecificAccountsCostPrice() {
        // ARRANGE
        final SupplementaryDataFactory supplementaryDataFactory =
            new SupplementaryDataFactory(CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
                    GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));
        final Set<Long> targetAccounts = Sets.newHashSet(1L);
        final float svc1Price = 1;
        final float svc2Price = 2;
        when(costBackend.getCurrentAccountExpenses(any()))
            .thenReturn(GetCurrentAccountExpensesResponse.newBuilder()
                .addAccountExpense(AccountExpenses.newBuilder()
                    .setAssociatedAccountId(1L)
                    .setAccountExpensesInfo(AccountExpensesInfo.newBuilder()
                        .addServiceExpenses(ServiceExpenses.newBuilder()
                            .setAssociatedServiceId(11)
                            .setExpenses(CurrencyAmount.newBuilder()
                                .setAmount(svc1Price)))
                        .addServiceExpenses(ServiceExpenses.newBuilder()
                            .setAssociatedServiceId(22)
                            .setExpenses(CurrencyAmount.newBuilder()
                                .setAmount(svc2Price)))))
                .build());

        // ACT
        final SupplementaryData data = supplementaryDataFactory.newSupplementaryData(
            targetAccounts, false);

        // ASSERT
        verify(costBackend).getCurrentAccountExpenses(GetCurrentAccountExpensesRequest.newBuilder()
            .setScope(AccountExpenseQueryScope.newBuilder()
                .setSpecificAccounts(IdList.newBuilder()
                    .addAllAccountIds(targetAccounts)))
            .build());
        assertThat(data.getCostPrice(1L), is(svc1Price + svc2Price));
    }

    /**
     * Test supplementary data contains count of resource groups owned by account.
     */
    @Test
    public void testSupplementaryDataFactoryGroupsOwnedByAccount() {
        final SupplementaryDataFactory supplementaryDataFactory = new SupplementaryDataFactory(
                CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel()), GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));
        final Integer countGroupsOwnedByAccount = 5;
        final Long accountID = 1L;
        final Set<Long> accountIds = Sets.newHashSet(accountID);
        when(groupServiceMole.countGroups(any())).thenReturn(CountGroupsResponse.newBuilder().setCount(countGroupsOwnedByAccount).build());
        final SupplementaryData data =
                supplementaryDataFactory.newSupplementaryData(accountIds, false);
        Assert.assertEquals(countGroupsOwnedByAccount, data.getResourceGroupCount(accountID));
    }

    /**
     * Test the "all" case of {@link SupplementaryDataFactory#newSupplementaryData(Set, boolean)}.
     */
    @Test
    public void testSupplementaryDataAllAccountsCostPrice() {
        // ARRANGE
        final SupplementaryDataFactory supplementaryDataFactory =
            new SupplementaryDataFactory(CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel()), GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));

        // ACT
        supplementaryDataFactory.newSupplementaryData(Collections.emptySet(), true);

        // ASSERT
        // Checking that we set the query scope properly.
        verify(costBackend).getCurrentAccountExpenses(GetCurrentAccountExpensesRequest.newBuilder()
            .setScope(AccountExpenseQueryScope.newBuilder()
                .setAll(true))
            .build());
    }

    /**
     * Test the empty case when using {@link BusinessAccountMapper}.
     */
    @Test
    public void testBusinessAccountMapperEmpty() {
        // ARRANGE
        final SupplementaryDataFactory supplementaryDataFactory = mock(SupplementaryDataFactory.class);
        // ACT
        final List<BusinessUnitApiDTO> retDtos = new BusinessAccountMapper(
            thinTargetCache, supplementaryDataFactory).convert(Collections.emptyList(), true);
        // ASSERT
        assertThat(retDtos, is(Collections.emptyList()));
    }

    /**
     * Test mapping a business account {@link TopologyEntityDTO} to a {@link BusinessUnitApiDTO}
     * via the {@link BusinessAccountMapper}.
     */
    @Test
    public void testBusinessAccountMapper() {
        // ARRANGE
        final long discoveringTarget = 99;
        final String accountId = "accountId";
        final String offerId = "offerId";
        final String enrollmentNum = "enrollment";
        final long subaccountId = 234234;
        final TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
            .setOid(ENTITY_OID)
            .setDisplayName(accountId)
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOrigin(Origin.newBuilder().setDiscoveryOrigin(
                DiscoveryOrigin.newBuilder()
                    .putDiscoveredTargetData(discoveringTarget, PerTargetEntityInformation.getDefaultInstance())))
            // Add an owned VM
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(123123)
                .setConnectionType(ConnectionType.OWNS_CONNECTION)
                .setConnectedEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
            // Add a sub-account
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(subaccountId)
                .setConnectionType(ConnectionType.OWNS_CONNECTION)
                .setConnectedEntityType(UIEntityType.BUSINESS_ACCOUNT.typeNumber()))
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setBusinessAccount(
                BusinessAccountInfo.newBuilder()
                    .setAssociatedTargetId(discoveringTarget)
                    .setAccountId(accountId)
                    .addPricingIdentifiers(PricingIdentifier.newBuilder()
                        .setIdentifierName(PricingIdentifierName.ENROLLMENT_NUMBER)
                        .setIdentifierValue(enrollmentNum).build())
                    .addPricingIdentifiers(PricingIdentifier.newBuilder()
                        .setIdentifierName(PricingIdentifierName.OFFER_ID)
                        .setIdentifierValue(offerId).build())))
            .build();

        final SupplementaryDataFactory supplementaryDataFactory = mock(SupplementaryDataFactory.class);
        final SupplementaryData supplementaryData = mock(SupplementaryData.class);
        when(supplementaryDataFactory.newSupplementaryData(any(), eq(false))).thenReturn(supplementaryData);
        final float costPrice = 11.0f;
        when(supplementaryData.getCostPrice(any())).thenReturn(costPrice);
        final BusinessAccountMapper businessAccountMapper =
            new BusinessAccountMapper(thinTargetCache, supplementaryDataFactory);

        final ThinTargetInfo discoveringTargetInfo = ImmutableThinTargetInfo.builder()
            .oid(discoveringTarget)
            .displayName("Discovering Target")
            .probeInfo(ImmutableThinProbeInfo.builder()
                .category(SDKProbeType.AWS.getProbeCategory().getCategory())
                .oid(11111)
                .type(SDKProbeType.AWS.getProbeType())
                .build())
            .isHidden(false)
            .build();
        when(thinTargetCache.getTargetInfo(discoveringTarget))
            .thenReturn(Optional.of(discoveringTargetInfo));


        // ACT
        final BusinessUnitApiDTO businessUnitDTO =
            businessAccountMapper.convert(Collections.singletonList(entity), false)
                // We expect one result.
                .stream().findFirst().get();

        // ASSERT
        verify(supplementaryDataFactory).newSupplementaryData(
                Collections.singleton(entity.getOid()), false);
        verify(supplementaryData).getCostPrice(entity.getOid());

        assertThat(businessUnitDTO.getBusinessUnitType(), is(BusinessUnitType.DISCOVERED));
        assertThat(businessUnitDTO.getUuid(), is(Long.toString(entity.getOid())));
        assertThat(businessUnitDTO.getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(businessUnitDTO.getClassName(), is(UIEntityType.BUSINESS_ACCOUNT.apiStr()));
        assertThat(businessUnitDTO.getCostPrice(), is(costPrice));
        assertThat(businessUnitDTO.getMemberType(), is(StringConstants.WORKLOAD));

        // 1 VM
        assertThat(businessUnitDTO.getMembersCount(), is(1));
        assertThat(businessUnitDTO.getChildrenBusinessUnits(), containsInAnyOrder(Long.toString(subaccountId)));
        assertThat(businessUnitDTO.getDisplayName(), is(accountId));


        assertThat(businessUnitDTO.getAccountId(), is(accountId));
        assertThat(businessUnitDTO.getAssociatedTargetId(), is(discoveringTarget));
        assertThat(businessUnitDTO.isMaster(), is(true));

        assertThat(businessUnitDTO.getPricingIdentifiers(), is(ImmutableMap.of(
            PricingIdentifierName.OFFER_ID.name(), offerId,
            PricingIdentifierName.ENROLLMENT_NUMBER.name(), enrollmentNum)));

        assertThat(businessUnitDTO.getCloudType(), is(CloudType.AWS));
        assertThat(businessUnitDTO.getTargets().size(), is(1));
        final TargetApiDTO targetApiDTO = businessUnitDTO.getTargets().get(0);
        assertThat(targetApiDTO.getType(), is(discoveringTargetInfo.probeInfo().type()));
        assertThat(targetApiDTO.getCategory(), is(discoveringTargetInfo.probeInfo().category()));
        assertThat(targetApiDTO.getDisplayName(), is(discoveringTargetInfo.displayName()));
        assertThat(targetApiDTO.getUuid(), is(Long.toString(discoveringTargetInfo.oid())));
    }
}

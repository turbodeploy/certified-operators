package com.vmturbo.api.component.external.api.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.PaginatedSearchRequest;
import com.vmturbo.api.component.communication.RepositoryApi.RepositoryRequestResult;
import com.vmturbo.api.component.external.api.mapper.EntityFilterMapper;
import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser;
import com.vmturbo.api.component.external.api.util.businessaccount.BusinessAccountMapper;
import com.vmturbo.api.component.external.api.util.businessaccount.SupplementaryData;
import com.vmturbo.api.component.external.api.util.businessaccount.SupplementaryDataFactory;
import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;
import com.vmturbo.api.dto.group.BillingFamilyApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.BusinessUnitType;
import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.pagination.SearchPaginationRequest;
import com.vmturbo.api.pagination.SearchPaginationRequest.SearchPaginationResponse;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenseQueryScope;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenseQueryScope.IdList;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesResponse;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupDTO.CountGroupsResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
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
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

/**
 * Unit tests for {@link BusinessAccountRetriever}.
 */
public class BusinessAccountRetrieverTest {

    private static final long ACCOUNT_OID = 7L;
    private static final String CHILD_ACCOUNT_1_ID_STR = "11";
    private static final String CHILD_ACCOUNT_2_ID_STR = "22";
    private static final String MASTER_ACCOUNT_1_ID_STR = "33";
    private static final String MASTER_ACCOUNT_2_ID_STR = "44";
    private static final BusinessUnitApiDTO CHILD_ACCOUNT_1 =
        createAccountDto(CHILD_ACCOUNT_1_ID_STR, false, Collections.emptyList());
    private static final BusinessUnitApiDTO CHILD_ACCOUNT_2 =
        createAccountDto(CHILD_ACCOUNT_2_ID_STR, false, Collections.emptyList());
    private static final BusinessUnitApiDTO MASTER_ACCOUNT_1 =
        createAccountDto(MASTER_ACCOUNT_1_ID_STR, true,
            Arrays.asList(CHILD_ACCOUNT_1.getAccountId(), CHILD_ACCOUNT_2.getAccountId()));
    private static final BusinessUnitApiDTO MASTER_ACCOUNT_2 =
        createAccountDto(MASTER_ACCOUNT_2_ID_STR, true, Collections.emptyList());

    private static final TopologyEntityDTO ACCOUNT = TopologyEntityDTO.newBuilder()
        .setOid(ACCOUNT_OID)
        .setDisplayName("monitored account")
        .setEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setBusinessAccount(BusinessAccountInfo.newBuilder()
                .setAssociatedTargetId(123L)
                .build())
            .build())
        .build();


    private ThinTargetCache thinTargetCache = mock(ThinTargetCache.class);

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

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

    private boolean defaultFetchCost = true;
    /**
     * Sets up the tests.
     */
    @Before
    public void init() {
        final GroupUseCaseParser groupUseCaseParser =
                new GroupUseCaseParser("groupBuilderUsecases.json");
        this.entityFilterMapper = new EntityFilterMapper(groupUseCaseParser, thinTargetCache);
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
     *
     * @throws OperationFailedException To satisfy compiler.
     * @throws InvalidOperationException To satisfy compiler.
     */
    @Test
    public void testGetBusinessAccountsInScopeNoTargets() throws OperationFailedException, InvalidOperationException {
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
        final SearchParameters searchParametersForAllAccounts = SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.entityTypeFilter(ApiEntityType.BUSINESS_ACCOUNT)).build();
        final SearchParameters searchParametersForFilteringMonitoredAccounts =
                getFilterForMonitoredAccounts();
        verify(repositoryApi).newSearchRequestMulti(
                Arrays.asList(searchParametersForAllAccounts, searchParametersForFilteringMonitoredAccounts));
        verify(mockMapper).convert(Collections.singletonList(ACCOUNT), true);

        assertThat(results, is(Collections.singletonList(convertedDto)));
    }

    /**
     * Test getting all business accounts when there are none in the system.
     *
     * @throws OperationFailedException To satisfy compiler.
     * @throws InvalidOperationException To satisfy compiler.
     */
    @Test
    public void testGetBusinessAccountsNone() throws OperationFailedException, InvalidOperationException {
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
     *
     * @throws OperationFailedException To satisfy compiler.
     * @throws InvalidOperationException To satisfy compiler.
     */
    @Test
    public void testGetBusinessAccountsInTargetScope() throws OperationFailedException, InvalidOperationException {
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
                                SearchProtoUtil.entityTypeFilter(ApiEntityType.BUSINESS_ACCOUNT))
                                .addSearchFilter(SearchProtoUtil.searchFilterProperty(
                                        SearchProtoUtil.discoveredBy(targetId)))
                                .build()));
        // "allAccounts" false, because we're scoped to a target.
        verify(mockMapper).convert(Collections.singletonList(ACCOUNT), false);

        assertThat(results, is(Collections.singletonList(convertedDto)));
    }

    /**
     * Test getting the business account that belongs to a valid group.
     *
     * @throws OperationFailedException To satisfy compiler.
     * @throws InvalidOperationException To satisfy compiler.
     */
    @Test
    public void testGetBusinessAccountsInScopeOfGroup() throws OperationFailedException, InvalidOperationException {
        // ARRANGE
        final String groupId = "123";
        final Set<Long> expandedOids = ImmutableSet.of(ACCOUNT_OID);

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
        final SearchParameters searchParametersForGroupScope = SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.entityTypeFilter(ApiEntityType.BUSINESS_ACCOUNT))
                .addSearchFilter(SearchProtoUtil.searchFilterProperty(
                        SearchProtoUtil.idFilter(expandedOids)))
                .build();
        final SearchParameters searchParametersForFilteringMonitoredAccounts =
                getFilterForMonitoredAccounts();
        verify(repositoryApi).newSearchRequestMulti(
                Arrays.asList(searchParametersForGroupScope, searchParametersForFilteringMonitoredAccounts));

        // Only ACCOUNT should be converted.
        // The UNMONITORED_ACCOUNT should not be converted because the account does not have an
        // associated target id, indicating that it was not monitored by a cloud probe. It was only
        // included as a member of a billing family.
        verify(mockMapper).convert(Lists.newArrayList(ACCOUNT), false);

        assertThat(results, is(Collections.singletonList(convertedDto)));
    }

    /**
     * Test that get business accounts request contains several search parameters. First for
     * searching  entities with business_account entity type and second for searching business
     * accounts which have associated target.
     *
     * @throws OperationFailedException To satisfy compiler.
     * @throws InvalidOperationException To satisfy compiler.
     */
    @Test
    public void testSearchParametersWhenGetBusinessAccountsInScope()
            throws OperationFailedException, InvalidOperationException {
        // ARRANGE
        final RepositoryApi.SearchRequest mockReq =
                ApiTestUtils.mockSearchFullReq(Collections.singletonList(ACCOUNT));
        when(repositoryApi.newSearchRequestMulti(any())).thenReturn(mockReq);

        final BusinessUnitApiDTO convertedDto = mock(BusinessUnitApiDTO.class);
        when(mockMapper.convert(any(), anyBoolean())).thenReturn(
                Collections.singletonList(convertedDto));

        final SearchParameters searchParameterForFilteringMonitoredAccounts =
                getFilterForMonitoredAccounts();

        // ACT
        businessAccountRetriever.getBusinessAccountsInScope(null, null);

        ArgumentCaptor<Collection> searchParametersCaptor =
                ArgumentCaptor.forClass((Collection.class));

        // ASSERT
        verify(repositoryApi).newSearchRequestMulti(searchParametersCaptor.capture());
        Assert.assertEquals(2, searchParametersCaptor.getValue().size());
        Assert.assertTrue(searchParametersCaptor.getValue()
                .contains(searchParameterForFilteringMonitoredAccounts));
    }

    /**
     * Test getting a single account by OID.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGetBusinessAccountByOid() throws Exception {
        final BusinessUnitApiDTO convertedDto = mock(BusinessUnitApiDTO.class);
        Mockito.when(repositoryApi.getByIds(Collections.singleton(ACCOUNT_OID),
                Collections.singleton(EntityType.BUSINESS_ACCOUNT), false))
                .thenReturn(new RepositoryRequestResult(Collections.singleton(convertedDto),
                        Collections.emptySet()));
        final BusinessUnitApiDTO result = businessAccountRetriever.getBusinessAccount(Long.toString(ACCOUNT_OID));
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
        Mockito.when(repositoryApi.getByIds(Collections.singleton(ACCOUNT_OID),
                Collections.singleton(EntityType.BUSINESS_ACCOUNT), false))
                .thenReturn(new RepositoryRequestResult(Collections.emptySet(),
                        Collections.emptySet()));
        businessAccountRetriever.getBusinessAccount(Long.toString(ACCOUNT_OID));
    }

    /**
     * Test getting a single account by OID.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGetBillingFamilies() throws Exception {
        // ARRANGE
        final TopologyEntityDTO parentAccount = TopologyEntityDTO.newBuilder()
            .setOid(ACCOUNT_OID + 1)
            .setDisplayName("bar")
            .setEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
                .setConnectedEntityId(ACCOUNT_OID)
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
        convertedChild.setUuid(Long.toString(ACCOUNT_OID));
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
            .setOid(ACCOUNT_OID + 1)
            .setDisplayName("bar")
            .setEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
            // Owns a VM, and a child account.
            .addConnectedEntities(ConnectedEntity.newBuilder()
                .setConnectedEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setConnectedEntityId(121212)
                .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .addConnectedEntities(ConnectedEntity.newBuilder()
                .setConnectedEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
                .setConnectedEntityId(ACCOUNT_OID)
                .setConnectionType(ConnectionType.OWNS_CONNECTION))
            .build();

        final RepositoryApi.SingleEntityRequest parentReq = ApiTestUtils.mockSingleEntityRequest(parentAccount);
        when(repositoryApi.entityRequest(anyLong())).thenReturn(parentReq);

        final BusinessUnitApiDTO convertedDto = mock(BusinessUnitApiDTO.class);
        Mockito.when(repositoryApi.getByIds(Collections.singleton(ACCOUNT_OID),
                Collections.singleton(EntityType.BUSINESS_ACCOUNT), false))
                .thenReturn(new RepositoryRequestResult(Collections.singleton(convertedDto),
                        Collections.emptySet()));
        // ACT
        final Collection<BusinessUnitApiDTO> results = businessAccountRetriever.getChildAccounts(Long.toString(parentAccount.getOid()));

        // ASSERT
        verify(repositoryApi).entityRequest(parentAccount.getOid());
        Assert.assertEquals(Collections.singleton(convertedDto), new HashSet<>(results));
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
            targetAccounts, false, defaultFetchCost);

        // ASSERT
        verify(costBackend).getCurrentAccountExpenses(GetCurrentAccountExpensesRequest.newBuilder()
            .setScope(AccountExpenseQueryScope.newBuilder()
                .setSpecificAccounts(IdList.newBuilder()
                    .addAllAccountIds(targetAccounts)))
            .build());
        Assert.assertEquals(Optional.of(svc1Price + svc2Price), data.getCostPrice(1L));
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
                supplementaryDataFactory.newSupplementaryData(accountIds, false, defaultFetchCost);
        Assert.assertEquals(countGroupsOwnedByAccount, data.getResourceGroupCount(accountID));
    }

    /**
     * Test the "all" case of {@link SupplementaryDataFactory#newSupplementaryData(Set, boolean, boolean)}.
     */
    @Test
    public void testSupplementaryDataAllAccountsCostPrice() {
        // ARRANGE
        final SupplementaryDataFactory supplementaryDataFactory =
            new SupplementaryDataFactory(CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel()), GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));

        // ACT
        supplementaryDataFactory.newSupplementaryData(Collections.emptySet(), true, defaultFetchCost);

        // ASSERT
        // Checking that we set the query scope properly.
        verify(costBackend).getCurrentAccountExpenses(GetCurrentAccountExpensesRequest.newBuilder()
            .setScope(AccountExpenseQueryScope.newBuilder()
                .setAllAccounts(true))
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
            thinTargetCache, supplementaryDataFactory, userSessionContext ).convert(Collections.emptyList(), true);
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
            .setOid(ACCOUNT_OID)
            .setDisplayName(accountId)
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOrigin(Origin.newBuilder().setDiscoveryOrigin(
                DiscoveryOrigin.newBuilder()
                    .putDiscoveredTargetData(discoveringTarget, PerTargetEntityInformation.getDefaultInstance())))
            // Add an owned VM
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(123123)
                .setConnectionType(ConnectionType.OWNS_CONNECTION)
                .setConnectedEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber()))
            // Add a sub-account
            .addConnectedEntityList(ConnectedEntity.newBuilder()
                .setConnectedEntityId(subaccountId)
                .setConnectionType(ConnectionType.OWNS_CONNECTION)
                .setConnectedEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber()))
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
        when(supplementaryDataFactory.newSupplementaryData(any(), eq(false), eq(true))).thenReturn(supplementaryData);
        final float costPrice = 11.0f;
        when(supplementaryData.getCostPrice(any())).thenReturn(Optional.of(costPrice));
        final BusinessAccountMapper businessAccountMapper =
            new BusinessAccountMapper(thinTargetCache, supplementaryDataFactory, userSessionContext );

        final ThinTargetInfo discoveringTargetInfo = ImmutableThinTargetInfo.builder()
            .oid(discoveringTarget)
            .displayName("Discovering Target")
            .probeInfo(ImmutableThinProbeInfo.builder()
                .category(ProbeCategory.CLOUD_MANAGEMENT.getCategory())
                .uiCategory(ProbeCategory.PUBLIC_CLOUD.getCategory())
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
                Collections.singleton(entity.getOid()), false, defaultFetchCost);
        verify(supplementaryData).getCostPrice(entity.getOid());

        assertThat(businessUnitDTO.getBusinessUnitType(), is(BusinessUnitType.DISCOVERED));
        assertThat(businessUnitDTO.getUuid(), is(Long.toString(entity.getOid())));
        assertThat(businessUnitDTO.getEnvironmentType(), is(EnvironmentType.CLOUD));
        assertThat(businessUnitDTO.getClassName(), is(ApiEntityType.BUSINESS_ACCOUNT.apiStr()));
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
        assertThat(targetApiDTO.getCategory(), is(discoveringTargetInfo.probeInfo().uiCategory()));
        assertThat(targetApiDTO.getDisplayName(), is(discoveringTargetInfo.displayName()));
        assertThat(targetApiDTO.getUuid(), is(Long.toString(discoveringTargetInfo.oid())));
    }

    /**
     * When cost is unavailable, {@link BusinessAccountMapper#convert(List, boolean)} should not throw an exception.
     * Instead costPrice should be null, indicating there is no cost price available. Cost price of
     * 0 is incorrect because it indicates there is no cost, when that might not be the case.
     */
    @Test
    public void testCostUnavailable() {
        final SupplementaryDataFactory supplementaryDataFactory = new SupplementaryDataFactory(
                CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
                GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));
        final BusinessAccountMapper mapper = new BusinessAccountMapper(thinTargetCache, supplementaryDataFactory, userSessionContext );

        doReturn(Optional.of(new StatusRuntimeException(Status.UNAVAILABLE))).when(costBackend).getCurrentAccountExpensesError(any());

        List<BusinessUnitApiDTO> actualBusinessUnits = mapper.convert(Collections.singletonList(ACCOUNT), true);
        Assert.assertEquals(1, actualBusinessUnits.size());
        BusinessUnitApiDTO actual = actualBusinessUnits.get(0);
        Assert.assertNull(actual.getCostPrice());
    }

    /**
     * When user is of type scoped observer; call to cost component
     * shud not be made to fetch account costs.
     */
    @Test
    public void testCostWithScopedUser() {
        final SupplementaryDataFactory supplementaryDataFactory = new SupplementaryDataFactory(
                CostServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
                GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));
        final BusinessAccountMapper mapper = new BusinessAccountMapper(thinTargetCache, supplementaryDataFactory, userSessionContext );
        when(userSessionContext.isUserObserver()).thenReturn(true);
        when(userSessionContext.isUserScoped()).thenReturn(true);
        // for scoped user we need atleast one connect workload entity.
        TopologyEntityDTO businessAccount = ACCOUNT.toBuilder().setEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)).build();
        List<BusinessUnitApiDTO> actualBusinessUnits = mapper.convert(Collections.singletonList(businessAccount), true);
        Assert.assertEquals(1, actualBusinessUnits.size());
        BusinessUnitApiDTO actual = actualBusinessUnits.get(0);
        Assert.assertNull(actual.getCostPrice());
    }

    /**
     * Test the case that a parent for a business account is retrieved.
     *
     * @throws InvalidOperationException if something goes wrong.
     * @throws OperationFailedException if something goes wrong.
     */
    @Test
    public void testGetParentBusinessAccount() throws InvalidOperationException,
            OperationFailedException {
        // ARRANGE
        final RepositoryApi.SearchRequest mockReq = ApiTestUtils.mockSearchFullReq(Collections.singletonList(ACCOUNT));
        when(repositoryApi.newSearchRequestMulti(any()))
            .thenReturn(mockReq);

        final BusinessUnitApiDTO convertedDto = mock(BusinessUnitApiDTO.class);
        when(mockMapper.convert(any(), anyBoolean()))
            .thenReturn(Arrays.asList(MASTER_ACCOUNT_1, MASTER_ACCOUNT_2,
                CHILD_ACCOUNT_1, CHILD_ACCOUNT_2));

        // ACT
        Optional<BusinessUnitApiDTO> parentAccount = businessAccountRetriever
            .getParentBusinessAccount(CHILD_ACCOUNT_1_ID_STR);

        // ASSERT
        Assert.assertTrue(parentAccount.isPresent());
        Assert.assertThat(parentAccount.get().getAccountId(), is(MASTER_ACCOUNT_1_ID_STR));
    }

    /**
     * Test the case that the parent business account is requested for a id which is not numeric.
     *
     * @throws InvalidOperationException if something goes wrong.
     * @throws OperationFailedException if something goes wrong.
     */
    @Test(expected = InvalidOperationException.class)
    public void testGetParentBusinessAccountNonNumericInput() throws InvalidOperationException,
        OperationFailedException {
        businessAccountRetriever.getParentBusinessAccount("test");
    }

    /**
     * Test the case that a parent for a business account is retrieved.
     *
     * @throws InvalidOperationException if something goes wrong.
     * @throws OperationFailedException if something goes wrong.
     * @throws ConversionException if something goes wrong.
     * @throws InterruptedException if something goes wrong.
     */
    @Test
    public void testGetSiblingBusinessAccount() throws InvalidOperationException,
        OperationFailedException, ConversionException, InterruptedException {
        // ARRANGE
        final RepositoryApi.SearchRequest mockReq = ApiTestUtils.mockSearchFullReq(Collections.singletonList(ACCOUNT));
        when(repositoryApi.newSearchRequestMulti(any()))
            .thenReturn(mockReq);

        final BusinessUnitApiDTO convertedDto = mock(BusinessUnitApiDTO.class);
        when(mockMapper.convert(any(), anyBoolean()))
            .thenReturn(Arrays.asList(MASTER_ACCOUNT_1, MASTER_ACCOUNT_2,
                CHILD_ACCOUNT_1, CHILD_ACCOUNT_2));

        Mockito.when(repositoryApi.getByIds(
            Collections.singletonList(Long.valueOf(CHILD_ACCOUNT_2_ID_STR)),
            Collections.singleton(EntityType.BUSINESS_ACCOUNT), false))
            .thenReturn(new RepositoryRequestResult(Collections.singleton(CHILD_ACCOUNT_2),
                Collections.emptySet()));

        // ACT
        Collection<BusinessUnitApiDTO> siblingCollection = businessAccountRetriever
            .getSiblingBusinessAccount(CHILD_ACCOUNT_1_ID_STR);

        // ASSERT
        Assert.assertThat(siblingCollection.size(), is(1));
        Assert.assertThat(siblingCollection.iterator().next().getAccountId(),
            is(CHILD_ACCOUNT_2_ID_STR));
    }

    /**
     * Test that get business accounts paginated request contains the right pagination parameters.
     *
     * @throws OperationFailedException To satisfy compiler.
     * @throws InvalidOperationException To satisfy compiler.
     */
    @Test
    public void testGetBusinessAccountsInScopePaginated()
            throws OperationFailedException, InvalidOperationException {
        // ARRANGE
        final String cursor = "5";
        int limit = 10;
        boolean ascending = true;
        final PaginatedSearchRequest mockPaginatedRequest = mock(PaginatedSearchRequest.class);
        final SearchPaginationResponse paginationResponse = mock(SearchPaginationResponse.class);
        when(mockPaginatedRequest.getBusinessUnitsResponse(anyBoolean())).thenReturn(paginationResponse);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(mockPaginatedRequest);
        final SearchPaginationRequest paginationRequest = new SearchPaginationRequest(cursor, limit, ascending,
                SearchOrderBy.SEVERITY.name());

        // ACT
        businessAccountRetriever.getBusinessAccountsInScope(null, null, paginationRequest);

        ArgumentCaptor<SearchPaginationRequest> searchPaginationRequestArgCaptor =
                ArgumentCaptor.forClass(SearchPaginationRequest.class);
        verify(repositoryApi).newPaginatedSearch(any(), any(), searchPaginationRequestArgCaptor.capture());

        // ASSERT
        Assert.assertEquals(cursor, searchPaginationRequestArgCaptor.getValue().getCursor().get());
        Assert.assertEquals(limit, searchPaginationRequestArgCaptor.getValue().getLimit());
        Assert.assertEquals(ascending, searchPaginationRequestArgCaptor.getValue().isAscending());
        Assert.assertEquals(SearchOrderBy.NAME, searchPaginationRequestArgCaptor.getValue().getOrderBy());
    }

    /**
     * Test that get business accounts paginated by Cloud Provider contains the right criteria parameters.
     *
     * @throws OperationFailedException To satisfy compiler.
     * @throws InvalidOperationException To satisfy compiler.
     */
    @Test
    public void testGetBusinessAccountsInScopePaginatedByCloudProvider()
            throws OperationFailedException, InvalidOperationException {
        // ARRANGE
        final PaginatedSearchRequest mockPaginatedRequest = mock(PaginatedSearchRequest.class);
        final SearchPaginationResponse paginationResponse = mock(SearchPaginationResponse.class);
        when(mockPaginatedRequest.getBusinessUnitsResponse(anyBoolean())).thenReturn(paginationResponse);
        when(repositoryApi.newPaginatedSearch(any(), any(), any())).thenReturn(mockPaginatedRequest);
        final SearchPaginationRequest paginationRequest = new SearchPaginationRequest("5", 3,
                true, null);
        final String filterType = "businessAccountCloudProvider";
        final String expVal = "AWS";
        final String expType = "EQ";
        FilterApiDTO filterApiDto = new FilterApiDTO();
        filterApiDto.setFilterType(filterType);
        filterApiDto.setExpVal(expVal);
        filterApiDto.setExpType(expType);

        // ACT
        businessAccountRetriever.getBusinessAccountsInScope(null, Lists.newArrayList(filterApiDto),
                paginationRequest);

        ArgumentCaptor<SearchQuery> searchQueryArgCaptor =
                ArgumentCaptor.forClass(SearchQuery.class);
        verify(repositoryApi).newPaginatedSearch(searchQueryArgCaptor.capture(), any(), any());
        SearchParameters.FilterSpecs filterSpecs = searchQueryArgCaptor.getValue().getSearchParametersList().stream()
                .filter(p -> p.getSourceFilterSpecs() != null
                        && p.getSourceFilterSpecs().getFilterType().equals(filterType))
                .map(p -> p.getSourceFilterSpecs())
                .findFirst().orElse(null);

        // ASSERT
        Assert.assertNotNull(filterSpecs);
        Assert.assertEquals(expType, filterSpecs.getExpressionType());
        Assert.assertEquals(expVal, filterSpecs.getExpressionValue());
    }

    private static BusinessUnitApiDTO createAccountDto(String stringId, boolean isMaster,
                                                       List<String> children) {
        BusinessUnitApiDTO businessUnitApiDTO = new BusinessUnitApiDTO();
        businessUnitApiDTO.setAccountId(stringId);
        businessUnitApiDTO.setMaster(isMaster);
        businessUnitApiDTO.setChildrenBusinessUnits(children);
        return businessUnitApiDTO;
    }

    private SearchParameters getFilterForMonitoredAccounts() {
        return SearchProtoUtil.makeSearchParameters(
                SearchProtoUtil.entityTypeFilter(ApiEntityType.BUSINESS_ACCOUNT))
                .addSearchFilter(SearchFilter.newBuilder()
                        .setPropertyFilter(SearchProtoUtil.associatedTargetFilter())
                        .build())
                .build();
    }
}

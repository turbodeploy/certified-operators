package com.vmturbo.api.component.external.api.util.action;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

public class ActionSearchUtilTest {

    private ActionSearchUtil actionSearchUtil;

    public ActionsServiceMole actionsServiceRpc = Mockito.spy(new ActionsServiceMole());
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsServiceRpc);
    public ActionsServiceBlockingStub actionOrchestratorRpc;

    @Mock
    private ActionSpecMapper actionSpecMapper;

    @Mock
    private PaginationMapper paginationMapper;

    @Mock
    private SupplyChainFetcherFactory supplyChainFetcherFactory;

    @Mock
    private GroupExpander groupExpander;

    @Mock
    ActionPaginationRequest paginationRequest;

    private static final long TOPOLOGY_ID = 1111L;
    private static final long RESOURCE_GROUP_ID = 111L;
    private static final long BILLING_FAMILY_ID = 222L;
    private static final long BUSINESS_ACCOUNT_ID_1 = 1L;
    private static final long BUSINESS_ACCOUNT_ID_2 = 2L;
    private static final Set<Long> BUSINESS_ACCOUNTS =
            ImmutableSet.of(BUSINESS_ACCOUNT_ID_1, BUSINESS_ACCOUNT_ID_2);

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        actionOrchestratorRpc = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());

        actionSearchUtil = new ActionSearchUtil(actionOrchestratorRpc, actionSpecMapper,
                paginationMapper, supplyChainFetcherFactory, groupExpander, TOPOLOGY_ID);

        Mockito.doReturn(FilteredActionResponse.newBuilder().build()).when(actionsServiceRpc)
                .getAllActions(any(FilteredActionRequest.class));

        when(actionSpecMapper.createActionFilter(any(), any(), any()))
                .thenReturn(ActionQueryFilter.getDefaultInstance());
        when(actionSpecMapper.mapActionSpecsToActionApiDTOs(any(), anyLong()))
                .thenReturn(Collections.emptyList());

        when(paginationMapper.toProtoParams(any(paginationRequest.getClass())))
                .thenReturn(PaginationParameters.getDefaultInstance());

        when(paginationRequest.nextPageResponse(any(), any(), any())).thenReturn(null);
        when(paginationRequest.finalPageResponse(any(), any())).thenReturn(null);
    }

    @Test
    public void testGetActionsByEntity() throws Exception {
        Set<Long> scope = Collections.singleton(BUSINESS_ACCOUNT_ID_1);
        ApiId scopeId = Mockito.mock(ApiId.class);
        when(scopeId.oid()).thenReturn(BUSINESS_ACCOUNT_ID_1);
        when(groupExpander.expandOids(scope)).thenReturn(scope);
        when(supplyChainFetcherFactory.expandAggregatingAndActionPropagatingEntities(scope)).thenReturn(scope);

        ActionApiInputDTO inputDto = Mockito.mock(ActionApiInputDTO.class);


        try {
            actionSearchUtil.getActionsByScope(scopeId, inputDto, paginationRequest);
        } catch (InterruptedException | UnsupportedActionException | OperationFailedException
                | ExecutionException e) {
            e.printStackTrace();
        }

        verify(supplyChainFetcherFactory, times(1))
                .expandAggregatingAndActionPropagatingEntities(any());
        verifyNoMoreInteractions(supplyChainFetcherFactory);
        verify(actionSpecMapper, times(1))
                .createActionFilter(any(), any(), any());
    }

    @Test
    public void testGetActionsByEntityGroup() throws Exception {
        ApiId scopeId = Mockito.mock(ApiId.class);
        when(scopeId.oid()).thenReturn(BILLING_FAMILY_ID);
        when(groupExpander.expandOids(Collections.singleton(BILLING_FAMILY_ID)))
                .thenReturn(BUSINESS_ACCOUNTS);
        when(supplyChainFetcherFactory.expandAggregatingAndActionPropagatingEntities(BUSINESS_ACCOUNTS))
                .thenReturn(BUSINESS_ACCOUNTS);

        ActionApiInputDTO inputDto = Mockito.mock(ActionApiInputDTO.class);

        try {
            actionSearchUtil.getActionsByScope(scopeId, inputDto, paginationRequest);
        } catch (InterruptedException | UnsupportedActionException | OperationFailedException
                | ExecutionException e) {
            e.printStackTrace();
        }

        verify(supplyChainFetcherFactory, times(1))
                .expandAggregatingAndActionPropagatingEntities(any());
        verifyNoMoreInteractions(supplyChainFetcherFactory);
        verify(actionSpecMapper, times(1))
                .createActionFilter(any(), any(), any());
    }

    @Test
    public void testGetActionsByEntityEmptyGroup() throws Exception {
        ApiId scopeId = Mockito.mock(ApiId.class);
        when(scopeId.oid()).thenReturn(RESOURCE_GROUP_ID);
        when(groupExpander.expandOids(Collections.singleton(RESOURCE_GROUP_ID)))
                .thenReturn(Collections.emptySet());

        ActionApiInputDTO inputDto = Mockito.mock(ActionApiInputDTO.class);

        try {
            actionSearchUtil.getActionsByScope(scopeId, inputDto, paginationRequest);
        } catch (InterruptedException | UnsupportedActionException | OperationFailedException
                | ExecutionException e) {
            e.printStackTrace();
        }

        verifyZeroInteractions(actionSpecMapper);
    }

    /**
     * This tests an action search with the following combination of factors:
     * <ul><li>scope on an empty group</li>
     *     <li>input specifies a "related entity type"</li>
     * </ul>
     * The expected result is empty.
     *
     * @throws Exception should not happen
     */
    @Test
    public void testGetActionsWithRelatedEntityTypeByEntityEmptyGroup() throws Exception {
        ApiId scopeId = Mockito.mock(ApiId.class);
        when(scopeId.oid()).thenReturn(RESOURCE_GROUP_ID);
        when(groupExpander.expandOids(Collections.singleton(RESOURCE_GROUP_ID)))
                .thenReturn(Collections.emptySet());

        ActionApiInputDTO inputDto = Mockito.mock(ActionApiInputDTO.class);
        when(inputDto.getRelatedEntityTypes())
                .thenReturn(Collections.singletonList(EntityType.VIRTUAL_MACHINE.name()));

        actionSearchUtil.getActionsByScope(scopeId, inputDto, paginationRequest);

        verifyZeroInteractions(actionSpecMapper);
    }

    @Test
    public void testGetActionsByEntityWithRelatedEntity() throws Exception {
        Set<Long> scope = Collections.singleton(BUSINESS_ACCOUNT_ID_1);
        ApiId scopeId = Mockito.mock(ApiId.class);
        when(scopeId.oid()).thenReturn(BUSINESS_ACCOUNT_ID_1);
        when(groupExpander.expandOids(scope)).thenReturn(scope);
        when(supplyChainFetcherFactory.expandScope(scope,
                Collections.singletonList(EntityType.VIRTUAL_MACHINE.name())))
                .thenReturn(scope);

        ActionApiInputDTO inputDto = Mockito.mock(ActionApiInputDTO.class);
        when(inputDto.getRelatedEntityTypes())
                .thenReturn(Collections.singletonList(EntityType.VIRTUAL_MACHINE.name()));

        try {
            actionSearchUtil.getActionsByScope(scopeId, inputDto, paginationRequest);
        } catch (InterruptedException | UnsupportedActionException | OperationFailedException
                | ExecutionException e) {
            e.printStackTrace();
        }

        verify(supplyChainFetcherFactory, times(1))
                .expandScope(any(), any());
        verifyNoMoreInteractions(supplyChainFetcherFactory);
        verify(actionSpecMapper, times(1))
                .createActionFilter(any(), any(), any());
    }
}

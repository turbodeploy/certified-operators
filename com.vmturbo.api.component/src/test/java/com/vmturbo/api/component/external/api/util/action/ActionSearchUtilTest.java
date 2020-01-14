package com.vmturbo.api.component.external.api.util.action;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.pagination.ActionPaginationRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.components.api.test.GrpcTestServer;

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
    private static final long BILLING_FAMILY_ID = 123L;
    private static final Set<Long> BUSINESS_ACCOUNTS = ImmutableSet.of(1L, 2L, 3L);

    @Before
    public void setup() throws InterruptedException, ExecutionException, UnsupportedActionException {
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

        when(groupExpander.expandOids(Collections.singleton(BILLING_FAMILY_ID)))
                .thenReturn(BUSINESS_ACCOUNTS);

        when(paginationMapper.toProtoParams(any(paginationRequest.getClass())))
                .thenReturn(PaginationParameters.getDefaultInstance());

        when(paginationRequest.nextPageResponse(any(), any(), any())).thenReturn(null);
        when(paginationRequest.finalPageResponse(any(), any())).thenReturn(null);
    }

    @Test
    public void testGetActionsByEntityBusinessAccount() {
        ApiId scopeId = Mockito.mock(ApiId.class);
        when(scopeId.isGroup()).thenReturn(false);

        ActionApiInputDTO inputDto = Mockito.mock(ActionApiInputDTO.class);

        try {
            actionSearchUtil.getActionsByEntity(scopeId, inputDto, paginationRequest);
        } catch (InterruptedException | OperationFailedException | UnsupportedActionException
                | ExecutionException e) {
            e.printStackTrace();
        }

        verify(groupExpander, times(0)).expandOids(any());
        verify(supplyChainFetcherFactory, times(1))
                .expandGroupingServiceEntities(any());
        verifyNoMoreInteractions(supplyChainFetcherFactory);
    }

    @Test
    public void testGetActionsByEntityBillingFamily() {
        ApiId scopeId = Mockito.mock(ApiId.class);
        Mockito.when(scopeId.isGroup()).thenReturn(true);

        ActionApiInputDTO inputDto = Mockito.mock(ActionApiInputDTO.class);

        try {
            actionSearchUtil.getActionsByEntity(scopeId, inputDto, paginationRequest);
        } catch (InterruptedException | OperationFailedException | UnsupportedActionException
                | ExecutionException e) {
            e.printStackTrace();
        }

        verify(groupExpander, times(1)).expandOids(any());
        verify(supplyChainFetcherFactory, times(1))
                .expandGroupingServiceEntities(any());
        verifyNoMoreInteractions(supplyChainFetcherFactory);
    }
}

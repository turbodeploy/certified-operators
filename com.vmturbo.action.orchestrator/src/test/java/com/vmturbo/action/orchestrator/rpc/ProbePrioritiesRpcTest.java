package com.vmturbo.action.orchestrator.rpc;

import java.util.List;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.stub.StreamObserver;

import com.vmturbo.action.orchestrator.action.ActionPaginator.ActionPaginatorFactory;
import com.vmturbo.action.orchestrator.action.ActionTypeToActionTypeCaseConverter;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTargetByProbeCategoryResolver;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.stats.LiveActionStatReader;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionProbePriorities;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionPrioritiesRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.GetActionPrioritiesResponse;

/**
 * Tests for getActionPrioroties rpc.
 */
public class ProbePrioritiesRpcTest {

    private ActionsRpcService actionsRpcService;

    private static final StreamObserver<GetActionPrioritiesResponse> TEST_OBSERVER =
            new ProbePrioritiesTestObserver();

    @Before
    public void setup() {
        actionsRpcService = new ActionsRpcService(Mockito.mock(ActionStorehouse.class),
                Mockito.mock(ActionExecutor.class),
                Mockito.mock(ActionTargetSelector.class),
                Mockito.mock(ActionTranslator.class),
                Mockito.mock(ActionPaginatorFactory.class),
                Mockito.mock(WorkflowStore.class),
                Mockito.mock(LiveActionStatReader.class));
    }

    /**
     * Tests action priorities for all action types. (Assert is in TestObserver)
     */
    @Test
    public void testGetActionPrioritiesForAllActionTypes() {
        actionsRpcService.getActionPriorities(GetActionPrioritiesRequest.newBuilder().build(), TEST_OBSERVER);
    }

    /**
     * Tests getActionPriorities for one cpecific action type. (Assert is in TestObserver)
     */
    @Test
    public void testGetActionPrioritiesForResizeActionType() {
        actionsRpcService.getActionPriorities(GetActionPrioritiesRequest.newBuilder().build(), TEST_OBSERVER);
    }

    /**
     * Implementation of StreamObserver to test getting of probePriorities.
     */
    private static class ProbePrioritiesTestObserver implements
            StreamObserver<GetActionPrioritiesResponse> {

        @Override
        public void onNext(@Nonnull GetActionPrioritiesResponse response) {
           for (ActionProbePriorities probePriorities : response.getActionProbePrioritiesList()) {
               checkActionProbePriorities(probePriorities);
           }
        }

        private void checkActionProbePriorities(@Nonnull ActionProbePriorities actionProbePriorities) {
            final ActionTypeCase actionTypeCase = ActionTypeToActionTypeCaseConverter
                    .getActionTypeCaseFor(actionProbePriorities.getActionType());
            final List<String> probePriorities =  ActionTargetByProbeCategoryResolver
                    .getProbePrioritiesFor(actionTypeCase);
            Assert.assertEquals(probePriorities, actionProbePriorities.getProbeCategoryList());
        }

        @Override
        public void onError(@Nonnull Throwable throwable) {

        }

        @Override
        public void onCompleted() {

        }
    }
}

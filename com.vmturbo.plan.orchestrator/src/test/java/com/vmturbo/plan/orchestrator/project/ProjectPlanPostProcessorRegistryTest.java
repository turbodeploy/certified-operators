package com.vmturbo.plan.orchestrator.project;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.plan.orchestrator.plan.PlanStatusListener.PlanStatusListenerException;

public class ProjectPlanPostProcessorRegistryTest {
    private static final long PLAN_ID = 7;

    @Captor
    private ArgumentCaptor<Consumer<ProjectPlanPostProcessor>> onCompleteCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testStatusChanged() throws PlanStatusListenerException {
        final ProjectPlanPostProcessorRegistry registry = new ProjectPlanPostProcessorRegistry();
        final ProjectPlanPostProcessor postProcessor = mock(ProjectPlanPostProcessor.class);
        when(postProcessor.getPlanId()).thenReturn(PLAN_ID);

        registry.registerPlanPostProcessor(postProcessor);

        final PlanInstance newPlanInstance = PlanInstance.newBuilder()
                .setPlanId(PLAN_ID)
                .setStatus(PlanStatus.READY)
                .build();
        registry.onPlanStatusChanged(newPlanInstance);

        verify(postProcessor).onPlanStatusChanged(eq(newPlanInstance));
        verify(postProcessor).registerOnCompleteHandler(any());
    }

    @Test
    public void testOnComplete() throws PlanStatusListenerException {
        final ProjectPlanPostProcessorRegistry registry = new ProjectPlanPostProcessorRegistry();
        final ProjectPlanPostProcessor postProcessor = mock(ProjectPlanPostProcessor.class);
        when(postProcessor.getPlanId()).thenReturn(PLAN_ID);

        registry.registerPlanPostProcessor(postProcessor);
        verify(postProcessor).registerOnCompleteHandler(onCompleteCaptor.capture());
        final Consumer<ProjectPlanPostProcessor> onCompleteHandler =
                onCompleteCaptor.getValue();
        onCompleteHandler.accept(postProcessor);

        final PlanInstance newPlanInstance = PlanInstance.getDefaultInstance().toBuilder()
                .setPlanId(PLAN_ID)
                .setStatus(PlanStatus.READY)
                .build();
        registry.onPlanStatusChanged(newPlanInstance);
        // Since we triggered the "on complete" callback, further status changes should not
        // propagate to the post processor.
        verify(postProcessor, never()).onPlanStatusChanged(any());
    }
}

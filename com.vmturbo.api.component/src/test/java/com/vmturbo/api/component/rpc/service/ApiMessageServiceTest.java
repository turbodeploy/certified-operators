package com.vmturbo.api.component.rpc.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.api.ApiMessage.ActionConversionRequest;
import com.vmturbo.common.protobuf.api.ApiMessage.ActionConversionResponse;

/**
 * Tests the {@link ApiMessageService} class.
 */
public class ApiMessageServiceTest {
    private static final long REALTIME_CONTEXT_ID = 77777L;

    private static final ActionSpec actionSpec = ActionSpec.newBuilder()
            .setRecommendationId(222)
            .build();

    @Mock
    private ActionSpecMapper actionSpecMapper;

    private ApiMessageService apiMessageService;

    /**
     * Sets up the test.
     *
     * @throws Exception if something goes wrong.
     */
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        apiMessageService = new ApiMessageService(actionSpecMapper, REALTIME_CONTEXT_ID);
    }

    /**
     * Tests the case where a action conversion request completed successfully.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void convertActionToApiMessage() throws Exception {
        // ARRANGE
        final ActionApiDTO actionApiDTO = new ActionApiDTO();
        actionApiDTO.setUuid("222");
        when(actionSpecMapper.mapActionSpecToActionApiDTO(actionSpec, REALTIME_CONTEXT_ID))
          .thenReturn(actionApiDTO);

        StreamObserver<ActionConversionResponse> responseObserver = mock(StreamObserver.class);

        // ACT
        apiMessageService.convertActionToApiMessage(ActionConversionRequest
                .newBuilder()
                .setActionSpec(actionSpec)
                .build(), responseObserver);

        // ASSERT
        ArgumentCaptor<ActionConversionResponse> response =
                ArgumentCaptor.forClass(ActionConversionResponse.class);
        verify(responseObserver, times(1)).onNext(response.capture());
        assertEquals("{\"uuid\":\"222\"}", response.getValue().getActionApiDto());
        verify(responseObserver, times(1)).onCompleted();
        verify(responseObserver, times(0)).onError(any());
    }

    /**
     * Tests the case where a action conversion request fails because of an exception in conversion logic.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void convertActionToApiMessageFailed() throws Exception {
        // ARRANGE
        when(actionSpecMapper.mapActionSpecToActionApiDTO(actionSpec, REALTIME_CONTEXT_ID))
                .thenThrow(new ConversionException("Test1"));

        StreamObserver<ActionConversionResponse> responseObserver = mock(StreamObserver.class);

        // ACT
        apiMessageService.convertActionToApiMessage(ActionConversionRequest
                .newBuilder()
                .setActionSpec(actionSpec)
                .build(), responseObserver);

        // ASSERT
        verify(responseObserver, times(0)).onNext(any());
        verify(responseObserver, times(0)).onCompleted();
        verify(responseObserver, times(1)).onError(any());
    }
}
package com.vmturbo.topology.processor.operation;

import static org.junit.Assert.assertEquals;

import java.time.Clock;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.KeepAlive;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.topology.processor.communication.BaseMessageHandler;
import com.vmturbo.topology.processor.communication.ExpiringMessageHandler.HandlerStatus;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.action.Action;
import com.vmturbo.topology.processor.operation.action.ActionMessageHandler;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryMessageHandler;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.operation.validation.ValidationMessageHandler;

/**
 * Tests for the {@link OperationMessageHandler}.
 */
public class OperationMessageHandlerTest {

    private final OperationManager operationManager = Mockito.mock(OperationManager.class);
    private final IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
    private final Discovery discovery = new Discovery(0, 0, identityProvider);

    /**
     * Test that the handler works properly when receiving a discovery response.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testReceiveDiscoveryResponse() throws Exception {
        final BaseMessageHandler handler = new DiscoveryMessageHandler(operationManager, discovery,
                Clock.systemUTC(), 10000);
        final MediationClientMessage clientMessage = MediationClientMessage.newBuilder()
                .setDiscoveryResponse(DiscoveryResponse.newBuilder()
                        .addEntityDTO(EntityDTO.newBuilder().setId("foo").setEntityType(EntityType.VIRTUAL_MACHINE))
                ).build();

        // Verify the handler says it is complete
        assertEquals(HandlerStatus.IN_PROGRESS, handler.onReceive(clientMessage));
        final MediationClientMessage clientMessage2 = MediationClientMessage.newBuilder()
                .setDiscoveryResponse(DiscoveryResponse.newBuilder()).build();
        final HandlerStatus status = handler.onReceive(clientMessage2);
        assertEquals(HandlerStatus.COMPLETE, status);

        // Verify result got queued.
        Mockito.verify(operationManager)
                .notifyDiscoveryResult(discovery, clientMessage.getDiscoveryResponse());
    }

    /**
     * Test that the handler extends its timeout when receiving a keepalive.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testReceiveKeepAlive() throws Exception {
        final Clock mockClock = Mockito.mock(Clock.class);

        Mockito.when(mockClock.millis())
                .thenReturn(1000L)
                .thenReturn(2000L);

        // Before receiving a refresh, the expected expiration time is
        // the first time+timeout=1000+1000=2000
        final BaseMessageHandler handler = new DiscoveryMessageHandler(operationManager, discovery, mockClock, 1000);
        assertEquals(2000L, handler.expirationTime());

        // Verify the handler says it is in progress
        final MediationClientMessage clientMessage = MediationClientMessage.newBuilder()
                .setKeepAlive(KeepAlive.newBuilder())
                .build();
        final HandlerStatus status = handler.onReceive(clientMessage);
        assertEquals(HandlerStatus.IN_PROGRESS, status);

        // After receiving the keep-alive, expected timeout is second time+timeout=2000+1000=3000
        assertEquals(3000L, handler.expirationTime());
    }

    /**
     * Test that the handler handles expiration properly.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testOnExpiration() throws Exception {
        final BaseMessageHandler handler = new DiscoveryMessageHandler(operationManager, discovery,
                Clock.systemUTC(), 1000);
        Thread.sleep(1000);
        handler.onExpiration();

        Mockito.verify(operationManager).notifyTimeout(discovery, 1);
    }

    /**
     * Test receiving an unexpected message.
     */
    @Test
    public void testDiscoveryWrongMessage() {
        final BaseMessageHandler handler = new DiscoveryMessageHandler(operationManager, discovery,
                Clock.systemUTC(), 1000);
        final MediationClientMessage clientMessage = MediationClientMessage.newBuilder()
                .setValidationResponse(ValidationResponse.newBuilder())
                .build();
        final HandlerStatus status = handler.onReceive(clientMessage);
        assertEquals(HandlerStatus.IN_PROGRESS, status);
    }

    /**
     * Test receiving an unexpected message.
     */
    @Test
    public void testValidationWrongMessage() {
        final Validation validation = new Validation(0, 0, identityProvider);
        final BaseMessageHandler handler = new ValidationMessageHandler(operationManager, validation,
                Clock.systemUTC(), 1000);
        final MediationClientMessage clientMessage = MediationClientMessage.newBuilder()
                .setDiscoveryResponse(DiscoveryResponse.newBuilder())
                .build();
        final HandlerStatus status = handler.onReceive(clientMessage);
        assertEquals(HandlerStatus.IN_PROGRESS, status);
    }

    /**
     * Test receiving an unexpected message.
     */
    @Test
    public void testActionWrongMessage() {
        final Action action = new Action(0, 0, 0, identityProvider);
        final BaseMessageHandler handler = new ActionMessageHandler(operationManager, action,
                Clock.systemUTC(), 1000);
        final MediationClientMessage clientMessage = MediationClientMessage.newBuilder()
                .setDiscoveryResponse(DiscoveryResponse.newBuilder())
                .build();
        final HandlerStatus status = handler.onReceive(clientMessage);
        assertEquals(HandlerStatus.IN_PROGRESS, status);
    }

}

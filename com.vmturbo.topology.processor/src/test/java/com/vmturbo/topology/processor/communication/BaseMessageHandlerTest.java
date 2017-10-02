package com.vmturbo.topology.processor.communication;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.time.Clock;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;

/**
 * Tests for the BaseMessageHandler class.
 */
public class BaseMessageHandlerTest {

    private final Clock mockClock = Mockito.mock(Clock.class);

    /**
     * Extension of BaseMessageHandler for testing.
     */
    private final class TestMessageHandler extends BaseMessageHandler {
        TestMessageHandler(Clock clock, long timeout) {
            super(clock, timeout);
        }

        public void onExpiration() {

        }

        @Nonnull
        public HandlerStatus onMessage(@Nonnull MediationClientMessage message) {
            return HandlerStatus.COMPLETE;
        }

        @Override
        public void onTransportClose() {}
    }

    @Test
    public void testExpirationTime() throws Exception {
        when(mockClock.millis()).thenReturn(1000L);
        BaseMessageHandler handler = new TestMessageHandler(mockClock, 1000);
        assertEquals(2000L, handler.expirationTime());
    }

    @Test
    public void testRefreshLastMessageTime() throws Exception {
        when(mockClock.millis())
            .thenReturn(1000L)
            .thenReturn(2000L);

        BaseMessageHandler handler = new TestMessageHandler(mockClock, 1000);
        handler.refreshLastMessageTime();
        assertEquals(3000L, handler.expirationTime());
    }
}
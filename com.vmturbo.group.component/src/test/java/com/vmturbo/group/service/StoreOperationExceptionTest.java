package com.vmturbo.group.service;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import java.nio.CharBuffer;

import io.grpc.Status;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link StoreOperationException} class.
 */
public class StoreOperationExceptionTest {
    private static final String TEST_MESSAGE = "Test exception message";

    /**
     * Tests truncation of message.
     */
    @Test
    public void getTruncatedMessage() {
        StoreOperationException exception = new StoreOperationException(Status.INTERNAL, TEST_MESSAGE);
        Assert.assertThat(exception.getTruncatedMessage(), is(TEST_MESSAGE));

        exception = new StoreOperationException(Status.INTERNAL, null);
        Assert.assertNull(exception.getTruncatedMessage());

        String longMessage = CharBuffer.allocate(10000).toString().replace( '\0', 'X' );
        exception = new StoreOperationException(Status.INTERNAL, longMessage);
        Assert.assertThat(exception.getTruncatedMessage(), endsWith("[THE REST IS TRUNCATED]"));
        Assert.assertThat(exception.getTruncatedMessage(), startsWith("XX"));
        Assert.assertThat(exception.getTruncatedMessage().length(), is(4000));
    }
}
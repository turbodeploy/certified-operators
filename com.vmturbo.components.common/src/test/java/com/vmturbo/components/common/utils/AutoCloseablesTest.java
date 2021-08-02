package com.vmturbo.components.common.utils;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;

import javax.annotation.Nullable;

import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link AutoCloseables}.
 */
public class AutoCloseablesTest {
    /**
     * Test that when there is no exception all resources get properly closed.
     *
     * @throws Exception on exception
     */
    @Test
    public void testNoException() throws Exception {
        final AutoCloseHelper a = Mockito.spy(new AutoCloseHelper(null));
        final AutoCloseHelper b = Mockito.spy(new AutoCloseHelper(null));
        int i = 0;
        try (AutoCloseables<Exception> autoCloseables = new AutoCloseables<>(Arrays.asList(a, b),
            (top) -> new Exception("Exception!", top))) {
            i++;
        }
        Mockito.verify(a).close();
        Mockito.verify(b).close();
    }

    /**
     * Test that when there is an exception during processing all internal resources get closed.
     *
     * @throws Exception on exception
     */
    @Test
    public void testExceptionDuringProcessing() throws Exception {
        final AutoCloseHelper a = Mockito.spy(new AutoCloseHelper(null));
        final AutoCloseHelper b = Mockito.spy(new AutoCloseHelper(null));
        final AutoCloseHelper c = Mockito.spy(new AutoCloseHelper(null));

        try (AutoCloseables<Exception> autoCloseables = new AutoCloseables<>(Arrays.asList(a, b, c),
            (top) -> new Exception("Exception!", top))) {
            throw new Exception("some exception");
        } catch (Exception e) {
            Mockito.verify(a).close();
            Mockito.verify(b).close();
            Mockito.verify(c).close();
        }
    }

    /**
     * Test that when there are multiple exceptions during close of some resources that all reosurces
     * still get closed.
     *
     * @throws Exception on exception
     */
    @Test
    public void testMultipleExceptions() throws Exception {
        final AutoCloseHelper a = Mockito.spy(new AutoCloseHelper("Exception in A"));
        final AutoCloseHelper b = Mockito.spy(new AutoCloseHelper(null));
        final AutoCloseHelper c = Mockito.spy(new AutoCloseHelper("Exception in C"));

        int i = 0;
        try (AutoCloseables<Exception> autoCloseables = new AutoCloseables<>(Arrays.asList(a, b, c),
            (top) -> new Exception("Exception!", top))) {
            i++;
        } catch (Exception e) {
            Mockito.verify(a).close();
            Mockito.verify(b).close();
            Mockito.verify(c).close();
            final StringWriter stackTrace = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTrace));

            assertThat(stackTrace.toString(), containsString("Exception!"));
            assertThat(stackTrace.toString(), containsString("Exception in A"));
            assertThat(stackTrace.toString(), containsString("Exception in C"));
            return;
        }
        fail();
    }

    /**
     * Helper autocloseable for use in these tests.
     */
    private static class AutoCloseHelper implements AutoCloseable {
        final String exceptionMessage;

        private AutoCloseHelper(@Nullable String exceptionMessage) {
            this.exceptionMessage = exceptionMessage;
        }

        @Override
        public void close() throws Exception {
            if (exceptionMessage != null) {
                throw new Exception(exceptionMessage);
            }
        }
    }
}

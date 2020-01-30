package com.vmturbo.components.common.diagnostics;

import java.io.IOException;
import java.util.zip.ZipOutputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/**
 * Unit test for {@link DiagnosticsAppender}.
 */
public class DiagnosticsAppenderTest {

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Tests how IO exception is treated inside diagnostics appender.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testExceptionOnWritingZip() throws Exception {
        final ZipOutputStream stream = Mockito.mock(ZipOutputStream.class);
        final DiagnosticsAppenderImpl appender =
                new DiagnosticsAppenderImpl(stream, "some-filename");
        Mockito.doThrow(new IOException("some exception")).when(stream).write(Mockito.any());
        expectedException.expect(DiagnosticsException.class);
        appender.appendString("somethind");
    }
}

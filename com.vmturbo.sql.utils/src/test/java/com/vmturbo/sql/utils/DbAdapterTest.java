package com.vmturbo.sql.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;

import com.google.common.base.Charsets;

import org.jooq.exception.SQLStateClass;
import org.junit.Test;

/**
 * Tests of {@link DbAdapter} class.
 */
public class DbAdapterTest {

    /**
     * Make sure our approach for preventing passwords showing up in stack traces works.
     *
     * @throws IOException if test throws
     */
    @Test
    public void testCopySQLExcetpionWithoutStack() throws IOException {
        SQLException e = new SQLException(
                "Sensitive Information: xyzzy", SQLStateClass.OTHER.name(), 101);
        SQLException copy = DbAdapter.copySQLExceptionWithoutStack("Benign message", e);
        assertEquals(SQLStateClass.OTHER.name(), copy.getSQLState());
        assertEquals(101, copy.getErrorCode());
        assertTrue(getStackTrace(copy).contains("Benign message"));
        assertTrue(getStackTrace(e).contains("xyzzy"));
        assertFalse(getStackTrace(copy).contains("xyzzy"));
        assertTrue(getStackTrace(new RuntimeException("foo", e)).contains("xyzzy"));
        assertFalse(getStackTrace(new RuntimeException("bar", copy)).contains("xyzzy"));
    }

    private String getStackTrace(Exception e) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        e.printStackTrace(new PrintStream(out));
        out.close();
        String stackTrace = out.toString(Charsets.UTF_8.name());
        return stackTrace;
    }
}
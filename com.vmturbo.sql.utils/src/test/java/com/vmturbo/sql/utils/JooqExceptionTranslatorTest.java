package com.vmturbo.sql.utils;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import java.sql.SQLException;

import org.jooq.Configuration;
import org.jooq.ExecuteContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.support.SQLExceptionTranslator;

/**
 * Tests the {@link JooqExceptionTranslator} class.
 */
public class JooqExceptionTranslatorTest {
    private static final String SQL_QUERY = "TEST QUERY";

    @Mock
    private ExecuteContext executeContext;

    @Mock
    private Configuration configuration;

    @Mock
    private SQLExceptionTranslator translator;

    private SQLException sqlException;

    private DataAccessException convertedException;

    /**
     * Sets up the test environment.
     */
    @Before
    public void testSetup() {
        MockitoAnnotations.initMocks(this);
        sqlException = new SQLException("Test SQL Exception");
        convertedException = new DataAccessException("Test") {};
        when(executeContext.sqlException()).thenReturn(sqlException);
        when(executeContext.sql()).thenReturn(SQL_QUERY);
        when(executeContext.configuration()).thenReturn(configuration);
        when(configuration.dialect()).thenReturn(SQLDialect.DEFAULT);
    }

    /**
     * Test the case where conversion using the translator is successful.
     */
    @Test
    public void testSuccessfulTranslation() {
        // ARRANGE
        ArgumentCaptor<RuntimeException> captor = ArgumentCaptor.forClass(RuntimeException.class);
        doNothing().when(executeContext).exception(captor.capture());

        when(translator.translate(eq("Access database using jOOQ"), eq(SQL_QUERY), eq(sqlException)))
                .thenReturn(convertedException);
        final JooqExceptionTranslator jooqTranslator = new JooqExceptionTranslator(t -> translator);

        // ACT
        jooqTranslator.exception(executeContext);

        // ASSERT
        Assert.assertThat(captor.getValue(), equalTo(convertedException));
    }

    /**
     * Test the case where conversion using the translator is successful.
     */
    @Test
    public void testTranslationReturnsNull() {
        // ARRANGE
        ArgumentCaptor<RuntimeException> captor = ArgumentCaptor.forClass(RuntimeException.class);
        doNothing().when(executeContext).exception(captor.capture());

        when(translator.translate(eq("Access database using jOOQ"), eq(SQL_QUERY), eq(sqlException)))
                .thenReturn(null);
        final JooqExceptionTranslator jooqTranslator = new JooqExceptionTranslator(t -> translator);

        // ACT
        jooqTranslator.exception(executeContext);

        // ASSERT
        Assert.assertThat(captor.getValue().getCause(), equalTo(sqlException));
    }

}
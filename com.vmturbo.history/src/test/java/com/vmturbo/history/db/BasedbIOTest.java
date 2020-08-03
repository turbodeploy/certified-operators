package com.vmturbo.history.db;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;

import org.jooq.exception.DataChangedException;
import org.jooq.exception.DataTypeException;
import org.junit.Test;

import com.vmturbo.history.db.BasedbIO.DBFailureType;

/**
 * Tests of BasedbIO class.
 */
public class BasedbIOTest {

    /**
     * Spot-check that the {@link BasedbIO#analyzeDBFailure(Exception)} method correctly
     * classifies exceptions.
     *
     * <p>Tested scenarios match logic in the method under test, making determination by:</p>
     * <ul>
     *     <li>Speicific excetpion type</li>
     *     <li>Full 5-character SQL state value</li>
     *     <li>2-character "class code" prefix shared by a class of SQL state values</li>
     * </ul>
     */
    @Test
    public void testDbFailureAnalysis() {
        // specific hard and soft exception classes
        Exception ex = new DataChangedException("");
        assertThat(BasedbIO.analyzeDBFailure(ex), is(DBFailureType.SOFT));
        ex = new DataTypeException("");
        assertThat(BasedbIO.analyzeDBFailure(ex), is(DBFailureType.HARD));
        // hard and soft 5-char sql states
        ex = new SQLException("", "IM004");
        assertThat(BasedbIO.analyzeDBFailure(ex), is(DBFailureType.SOFT));
        ex = new SQLException("", "40002");
        assertThat(BasedbIO.analyzeDBFailure(ex), is(DBFailureType.HARD));
        // hard and soft 2-char class codes
        ex = new SQLException("", "08");
        assertThat(BasedbIO.analyzeDBFailure(ex), is(DBFailureType.SOFT));
        ex = new SQLException("", "03");
        assertThat(BasedbIO.analyzeDBFailure(ex), is(DBFailureType.HARD));
        // strange 2-char class class code (there are no strange 5-char sql states)
        ex = new SQLException("", "00");
        assertThat(BasedbIO.analyzeDBFailure(ex), is(DBFailureType.STRANGE));
        // fall-through tests: 5-char sql states for which we don't have direct
        // entries and so fall through to class code
        ex = new SQLException("", "IM000");
        assertThat(BasedbIO.analyzeDBFailure(ex), is(DBFailureType.HARD));
        ex = new SQLException("", "HY000");
        assertThat(BasedbIO.analyzeDBFailure(ex), is(DBFailureType.SOFT));
        ex = new SQLException("", "00000");
        assertThat(BasedbIO.analyzeDBFailure(ex), is(DBFailureType.STRANGE));
        // unparseable SQL state => SOFT
        ex = new SQLException("", "xxx");
        assertThat(BasedbIO.analyzeDBFailure(ex), is(DBFailureType.SOFT));
    }
}

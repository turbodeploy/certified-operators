package com.vmturbo.platform.analysis.ledger;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * Tests for the {@link IncomeStatement} class.
 */
@RunWith(JUnitParamsRunner.class)
public class IncomeStatementTest {
    // Fields
    private IncomeStatement fixture_;

    // Methods

    @Before
    public void setUp() {

        fixture_ = new IncomeStatement();
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: (set|get)Revenues({0})")
    public final void testSetGetRevenues_NormalInput(int revenues) {
        fixture_.setRevenues(revenues);
        assertEquals(revenues, fixture_.getRevenues(), 0);
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: (set|get)Expenses({0})")
    public final void testSetGetExpenses_NormalInput(int expenses) {
        fixture_.setExpenses(expenses);
        assertEquals(expenses, fixture_.getExpenses(), 0);
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: (set|get)MinDesiredExpenses({0})")
    public final void testSetGetMinDesiredExpenses_NormalInput(int minDesiredExpenses) {
        fixture_.setMinDesiredExpenses(minDesiredExpenses);
        assertEquals(minDesiredExpenses, fixture_.getMinDesiredExpenses(), 0);
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: (set|get)MaxDesiredExpenses({0})")
    public final void testSetGetMaxDesiredExpenses_NormalInput(int maxDesiredExpenses) {
        fixture_.setMaxDesiredExpenses(maxDesiredExpenses);
        assertEquals(maxDesiredExpenses, fixture_.getMaxDesiredExpenses(), 0);
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: (set|get)MinDesiredRevenues({0})")
    public final void testSetGetMinDesiredRevenues_NormalInput(int minDesiredRevenues) {
        fixture_.setMinDesiredRevenues(minDesiredRevenues);
        assertEquals(minDesiredRevenues, fixture_.getMinDesiredRevenues(), 0);
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: (set|get)MaxDesiredRevenues({0})")
    public final void testSetGetMaxDesiredRevenues_NormalInput(int maxDesiredRevenues) {
        fixture_.setMaxDesiredRevenues(maxDesiredRevenues);
        assertEquals(maxDesiredRevenues, fixture_.getMaxDesiredRevenues(), 0);
    }
    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-1","-2","-100","-2147483648"})
    @TestCaseName("Test #{index}: setRevenues({0})")
    public final void testSetRevenues_InvalidInput(int revenues) {
        fixture_.setRevenues(revenues);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-1","-2","-100","-2147483648"})
    @TestCaseName("Test #{index}: setExpenses({0})")
    public final void testSetExpenses_InvalidInput(int expenses) {
        fixture_.setExpenses(expenses);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-1","-2","-100","-2147483648"})
    @TestCaseName("Test #{index}: setMinDesiredExpenses({0})")
    public final void testSetMinDesiredExpenses_InvalidInput(int minDesiredExpenses) {
        fixture_.setMinDesiredExpenses(minDesiredExpenses);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-1","-2","-100","-2147483648"})
    @TestCaseName("Test #{index}: setMaxDesiredExpenses({0})")
    public final void testSetMaxDesiredExpenses_InvalidInput(int maxDesiredExpenses) {
        fixture_.setMaxDesiredRevenues(maxDesiredExpenses);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-1","-2","-100","-2147483648"})
    @TestCaseName("Test #{index}: setMinDesiredRevenues({0})")
    public final void testSetMinDesiredRevenues_InvalidInput(int minDesiredRevenues) {
        fixture_.setMinDesiredRevenues(minDesiredRevenues);
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters({"-1","-2","-100","-2147483648"})
    @TestCaseName("Test #{index}: setMaxDesiredRevenues({0})")
    public final void testSetMaxDesiredRevenues_InvalidInput(int maxDesiredRevenues) {
        fixture_.setMaxDesiredRevenues(maxDesiredRevenues);
    }

} // end IncomeStatementTest class

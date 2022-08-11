package com.vmturbo.platform.analysis.ledger;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import com.vmturbo.platform.analysis.testUtilities.TestUtils;

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
        assertEquals(revenues, fixture_.getRevenues(), TestUtils.FLOATING_POINT_DELTA);
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: (set|get)Expenses({0})")
    public final void testSetGetExpenses_NormalInput(int expenses) {
        fixture_.setExpenses(expenses);
        assertEquals(expenses, fixture_.getExpenses(), TestUtils.FLOATING_POINT_DELTA);
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: (set|get)MinDesiredExpenses({0})")
    public final void testSetGetMinDesiredExpenses_NormalInput(int minDesiredExpenses) {
        fixture_.setMinDesiredExpenses(minDesiredExpenses);
        assertEquals(minDesiredExpenses, fixture_.getMinDesiredExpenses(), TestUtils.FLOATING_POINT_DELTA);
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: (set|get)MaxDesiredExpenses({0})")
    public final void testSetGetMaxDesiredExpenses_NormalInput(int maxDesiredExpenses) {
        fixture_.setMaxDesiredExpenses(maxDesiredExpenses);
        assertEquals(maxDesiredExpenses, fixture_.getMaxDesiredExpenses(), TestUtils.FLOATING_POINT_DELTA);
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: (set|get)MinDesiredRevenues({0})")
    public final void testSetGetMinDesiredRevenues_NormalInput(int minDesiredRevenues) {
        fixture_.setMinDesiredRevenues(minDesiredRevenues);
        assertEquals(minDesiredRevenues, fixture_.getMinDesiredRevenues(), TestUtils.FLOATING_POINT_DELTA);
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: (set|get)MaxDesiredRevenues({0})")
    public final void testSetGetMaxDesiredRevenues_NormalInput(int maxDesiredRevenues) {
        fixture_.setMaxDesiredRevenues(maxDesiredRevenues);
        assertEquals(maxDesiredRevenues, fixture_.getMaxDesiredRevenues(), TestUtils.FLOATING_POINT_DELTA);
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

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: getROI({0})")
    public final void testGetROI_NormalInput(int revenues) {
        fixture_.setRevenues(revenues);
        fixture_.setExpenses(0);
        assertEquals(fixture_.getROI(), fixture_.getRevenues(), TestUtils.FLOATING_POINT_DELTA);
        fixture_.setExpenses(100);
        assertEquals(fixture_.getROI(), fixture_.getRevenues()/fixture_.getExpenses(), TestUtils.FLOATING_POINT_DELTA);
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: getMinDesiredROI({0})")
    public final void testGetMinDesiredROI_NormalInput(int minDesiredRevenues) {
        fixture_.setMinDesiredRevenues(minDesiredRevenues);
        fixture_.setMaxDesiredExpenses(0);
        assertEquals(fixture_.getMinDesiredROI(), fixture_.getMinDesiredRevenues(), TestUtils.FLOATING_POINT_DELTA);
        fixture_.setMaxDesiredExpenses(100);
        assertEquals(fixture_.getMinDesiredROI(), fixture_.getMinDesiredRevenues()/fixture_.getMaxDesiredExpenses(), TestUtils.FLOATING_POINT_DELTA);
    }

    @Test
    @Parameters({"0","1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: getMaxDesiredROI({0})")
    public final void testGetMaxDesiredROI_NormalInput(int maxDesiredRevenues) {
        fixture_.setMaxDesiredRevenues(maxDesiredRevenues);
        fixture_.setMinDesiredExpenses(0);
        assertEquals(fixture_.getMaxDesiredROI(), fixture_.getMaxDesiredRevenues(), TestUtils.FLOATING_POINT_DELTA);
        fixture_.setMinDesiredExpenses(100);
        assertEquals(fixture_.getMaxDesiredROI(), fixture_.getMaxDesiredRevenues()/fixture_.getMinDesiredExpenses(), TestUtils.FLOATING_POINT_DELTA);
    }

    @Test
    @Parameters({"1","10","100","1000","2147483647"})
    @TestCaseName("Test #{index}: resetIncomeStatement({0})")
    public final void testResetIncomeStatement_NormalInput(int num) {
        fixture_.setExpenses(num);
        fixture_.setRevenues(num);
        fixture_.setMaxDesiredExpenses(num);
        fixture_.setMaxDesiredRevenues(num);
        fixture_.setMinDesiredExpenses(num);
        fixture_.setMinDesiredRevenues(num);
        assertFalse(fixture_.getExpenses() == 0);
        assertFalse(fixture_.getRevenues() == 0);
        assertFalse(fixture_.getMaxDesiredExpenses() == 0);
        assertFalse(fixture_.getMaxDesiredRevenues() == 0);
        assertFalse(fixture_.getMinDesiredExpenses() == 0);
        assertFalse(fixture_.getMinDesiredRevenues() == 0);

        fixture_.resetIncomeStatement();
        assertTrue(fixture_.getExpenses() == 0);
        assertTrue(fixture_.getRevenues() == 0);
        assertTrue(fixture_.getMaxDesiredExpenses() == 0);
        assertTrue(fixture_.getMaxDesiredRevenues() == 0);
        assertTrue(fixture_.getMinDesiredExpenses() == 0);
        assertTrue(fixture_.getMinDesiredRevenues() == 0);
    }
} // end IncomeStatementTest class

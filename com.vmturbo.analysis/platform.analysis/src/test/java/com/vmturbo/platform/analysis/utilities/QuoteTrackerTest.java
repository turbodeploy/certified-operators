package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.analysis.economy.ShoppingList;

/**
 * Tests for {@link QuoteTracker} test.
 */
public class QuoteTrackerTest {
    private final ShoppingList shoppingList = Mockito.mock(ShoppingList.class);
    private final QuoteTracker quoteTracker = new QuoteTracker(shoppingList);
    private final QuoteTracker otherQuoteTracker = new QuoteTracker(shoppingList);

    private final Quote rank1Quote = Mockito.mock(Quote.class);
    private final Quote rank2Quote = Mockito.mock(Quote.class);
    private final Quote placedQuote = Mockito.mock(Quote.class);

    @Before
    public void setup() {
        when(rank1Quote.getRank()).thenReturn(1);
        when(rank2Quote.getRank()).thenReturn(2);
        when(placedQuote.getRank()).thenReturn(0);
    }

    @Test
    public void testHasQuotesToExplain() {
        assertFalse(quoteTracker.hasQuotesToExplain());

        quoteTracker.trackQuote(rank1Quote);
        assertTrue(quoteTracker.hasQuotesToExplain());
    }

    @Test
    public void testPreserveLowerRank() {
        assertEquals(Quote.INVALID_RANK, quoteTracker.getQuoteRank());

        quoteTracker.trackQuote(rank1Quote);
        assertEquals(1, quoteTracker.getQuoteRank());

        quoteTracker.trackQuote(rank2Quote);
        assertEquals(1, quoteTracker.getQuoteRank());
    }

    @Test
    public void testTakeOnLowerRank() {
        assertEquals(Quote.INVALID_RANK, quoteTracker.getQuoteRank());

        quoteTracker.trackQuote(rank2Quote);
        assertEquals(2, quoteTracker.getQuoteRank());

        quoteTracker.trackQuote(rank1Quote);
        assertEquals(1, quoteTracker.getQuoteRank());
    }

    @Test
    public void testGetInfiniteQuotesToExplain() {
        assertTrue(quoteTracker.getInfiniteQuotesToExplain().isEmpty());

        quoteTracker.trackQuote(rank2Quote);
        assertEquals(Collections.singletonList(rank2Quote), quoteTracker.getInfiniteQuotesToExplain());

        quoteTracker.trackQuote(rank1Quote);
        assertEquals(Collections.singletonList(rank1Quote), quoteTracker.getInfiniteQuotesToExplain());
    }

    @Test
    public void testTrackingPlacedQuote() {
        assertFalse(quoteTracker.successfullyPlaced());

        quoteTracker.trackQuote(rank1Quote);
        assertFalse(quoteTracker.successfullyPlaced());

        quoteTracker.trackQuote(placedQuote);
        assertTrue(quoteTracker.successfullyPlaced());

        // Tracking an additional infinite quote after tracking a successfully placed quote should
        // not change the tracker. The tracker should continue to track the placed quote.
        quoteTracker.trackQuote(rank1Quote);
        assertTrue(quoteTracker.successfullyPlaced());
    }

    @Test
    public void testBecomesValidAfterTrackingUnplaced() {
        assertFalse(quoteTracker.isValid());

        quoteTracker.trackQuote(rank2Quote);
        assertTrue(quoteTracker.isValid());
    }

    @Test
    public void testBecomesValidAfterTrackingPlaced() {
        assertFalse(quoteTracker.isValid());

        quoteTracker.trackQuote(placedQuote);
        assertTrue(quoteTracker.isValid());
    }

    @Test
    public void testMultipleQuotesOfSameRank() {
        final Quote secondRank1Quote = Mockito.mock(Quote.class);
        final Quote thirdRank1Quote = Mockito.mock(Quote.class);

        when(secondRank1Quote.getRank()).thenReturn(1);
        when(thirdRank1Quote.getRank()).thenReturn(1);

        assertEquals(0, quoteTracker.getInfiniteQuotesToExplain().size());

        quoteTracker.trackQuote(rank1Quote);
        assertEquals(1, quoteTracker.getInfiniteQuotesToExplain().size());

        quoteTracker.trackQuote(rank1Quote);
        assertEquals(2, quoteTracker.getInfiniteQuotesToExplain().size());

        quoteTracker.trackQuote(rank1Quote);
        assertEquals(3, quoteTracker.getInfiniteQuotesToExplain().size());
    }

    @Test
    public void testCombineWithLowerRank() {
        quoteTracker.trackQuote(rank2Quote);
        otherQuoteTracker.trackQuote(rank1Quote);
        assertNotEquals(Collections.singletonList(rank1Quote), quoteTracker.getInfiniteQuotesToExplain());

        quoteTracker.combine(otherQuoteTracker);
        assertEquals(Collections.singletonList(rank1Quote), quoteTracker.getInfiniteQuotesToExplain());
    }

    @Test
    public void testCombineWithSameRank() {
        final Quote secondRank1Quote = Mockito.mock(Quote.class);
        when(secondRank1Quote.getRank()).thenReturn(1);

        quoteTracker.trackQuote(rank1Quote);
        otherQuoteTracker.trackQuote(secondRank1Quote);
        assertNotEquals(Arrays.asList(rank1Quote, secondRank1Quote),
            quoteTracker.getInfiniteQuotesToExplain());

        quoteTracker.combine(otherQuoteTracker);
        assertEquals(Arrays.asList(rank1Quote, secondRank1Quote),
            quoteTracker.getInfiniteQuotesToExplain());
    }

    @Test
    public void testCombineWithHigherRank() {
        quoteTracker.trackQuote(rank1Quote);
        otherQuoteTracker.trackQuote(rank2Quote);
        assertEquals(Collections.singletonList(rank1Quote),
            quoteTracker.getInfiniteQuotesToExplain());

        quoteTracker.combine(otherQuoteTracker);
        assertEquals(Collections.singletonList(rank1Quote),
            quoteTracker.getInfiniteQuotesToExplain());
    }

    @Test
    public void testCombineWithSuccessfullyPlaced() {
        quoteTracker.trackQuote(rank1Quote);
        otherQuoteTracker.trackQuote(placedQuote);
        assertNotEquals(Collections.emptyList(), quoteTracker.getInfiniteQuotesToExplain());

        quoteTracker.combine(otherQuoteTracker);
        assertEquals(Collections.emptyList(), quoteTracker.getInfiniteQuotesToExplain());
    }

    @Test
    public void testCombineStartingInvalid() {
        otherQuoteTracker.trackQuote(rank2Quote);
        assertFalse(quoteTracker.isValid());

        quoteTracker.combine(otherQuoteTracker);
        assertTrue(quoteTracker.isValid());
        assertEquals(Collections.singletonList(rank2Quote),
            quoteTracker.getInfiniteQuotesToExplain());
    }

    @Test
    public void testCombineWithInvalid() {
        quoteTracker.trackQuote(rank2Quote);
        assertTrue(quoteTracker.isValid());
        assertFalse(otherQuoteTracker.isValid());

        quoteTracker.combine(otherQuoteTracker);
        assertTrue(quoteTracker.isValid());
        assertEquals(Collections.singletonList(rank2Quote),
            quoteTracker.getInfiniteQuotesToExplain());
    }

    @Test
    public void testBothInvalid() {
        assertFalse(quoteTracker.isValid());
        assertFalse(otherQuoteTracker.isValid());

        quoteTracker.combine(otherQuoteTracker);
        assertFalse(quoteTracker.isValid());
        assertFalse(otherQuoteTracker.isValid());
    }
}
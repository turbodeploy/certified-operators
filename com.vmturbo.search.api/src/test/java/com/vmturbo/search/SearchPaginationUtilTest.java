package com.vmturbo.search;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

/**
 * Tests SearchPaginationUtil.
 */
public class SearchPaginationUtilTest {

    /**
     * Test unparsing of cursor value.
     */
    @Test
    public void testParsingCursor() {
        //GIVEN
        String cursor = "val1|val2|val3";

        //WHEN
        List<String> results = SearchPaginationUtil.parseCursor(cursor);

        //THEN
        assertEquals(results.size(), 3);
    }

}

package com.vmturbo.common.protobuf;

import static com.vmturbo.common.protobuf.ListUtil.mergeTwoSortedListsAndRemoveDuplicates;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/**
 * Test the correctness of list utilities.
 */
public class ListUtilTest {

    /**
     * Test {@link ListUtil#mergeTwoSortedListsAndRemoveDuplicates} with Long input.
     */
    @Test
    public void testMergeTwoSortedListsAndRemoveDuplicatesLong() {
        List<Long> list1 = Arrays.asList(1L, 3L, 5L, 6L, 7L);
        List<Long> list2 = Arrays.asList(3L, 4L, 6L);
        assertEquals(Arrays.asList(1L, 3L, 4L, 5L, 6L, 7L),
            mergeTwoSortedListsAndRemoveDuplicates(list1, list2));
    }
}

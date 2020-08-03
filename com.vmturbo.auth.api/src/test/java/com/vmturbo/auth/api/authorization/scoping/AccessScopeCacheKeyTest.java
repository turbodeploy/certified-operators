package com.vmturbo.auth.api.authorization.scoping;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test the access scope cache key behavior -- mainly we want to verify the hash codes work, since
 * we are using it as a hash map key.
 */
@RunWith(Parameterized.class)
public class AccessScopeCacheKeyTest {
    @Parameters
    public static Collection<Object[]> scenarios(){
        // test scenarios for matching scope hash keys
        return Arrays.asList(new Object[][] {
                {false, Collections.emptyList(), false, Collections.emptyList(), true},
                {true, Collections.emptyList(), false, Collections.emptyList(), false},
                {true, Arrays.asList(1L, 2L), false, Arrays.asList(1L, 2L), false},
                // order independence
                {true, Arrays.asList(1L, 2L), true, Arrays.asList(1L, 2L), true},
                {true, Arrays.asList(2L, 1L), true, Arrays.asList(1L, 2L), true},
                // overflow checks
                {true, Arrays.asList(Long.MAX_VALUE, Long.MAX_VALUE - 1), true, Arrays.asList(Long.MAX_VALUE, Long.MAX_VALUE - 2), false},
                {true, Arrays.asList(Long.MAX_VALUE, Long.MAX_VALUE - 1), true, Arrays.asList(Long.MAX_VALUE - 1, Long.MAX_VALUE), true},
                // deduplication
                {true, Arrays.asList(1L, 2L), true, Arrays.asList(1L, 2L, 1L), true}
        });
    }

    private final AccessScopeCacheKey keyA;
    private final AccessScopeCacheKey keyB;
    private final boolean expectMatch;

    public AccessScopeCacheKeyTest(boolean flagA, List<Long> listA, boolean flagB, List<Long> listB, boolean expectMatch) {
        keyA = new AccessScopeCacheKey(listA, flagA);
        keyB = new AccessScopeCacheKey(listB, flagB);
        this.expectMatch = expectMatch;
    }

    @Test
    public void testAccessScopeCacheKeys() {
        // we expect both hashCode and equals to return the same results
        if (expectMatch) {
            Assert.assertEquals(keyA.hashCode(), keyB.hashCode());
            Assert.assertEquals(keyA, keyB);
        } else {
            Assert.assertNotEquals(keyA.hashCode(), keyB.hashCode());
            Assert.assertNotEquals(keyA, keyB);
        }
    }

}

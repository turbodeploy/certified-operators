package com.vmturbo.extractor.topology;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.Random;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.junit.Test;

/**
 * Test class for {@link EntityIdManager}.
 */
public class EntityIdManagerTest {
    private static final Random rand = new Random();
    private final EntityIdManager entityIdManager = new EntityIdManager();
    long[] oids = new long[10000];
    LongSet repeatCheck = new LongOpenHashSet();

    /**
     * Test that a bunch of random longs are assigned consecutive iids, and that converting back is
     * successful.
     */
    @Test
    public void testMappingsAreReversible() {
        for (int i = 0; i < oids.length; i++) {
            oids[i] = getRandomOid();
            assertThat(entityIdManager.toIid(oids[i]), is(i + 1));
        }
        for (int i = 0; i < oids.length; i++) {
            // check that a second oid conversion results in the same iid
            assertThat(entityIdManager.toIid(oids[i]), is(i + 1));
            // and of course that the reverse conversion works
            assertThat(entityIdManager.toOid(i + 1), is(oids[i]));
        }
    }

    /**
     * Test that the non-throwing iid-to-oid conversion method returns null when given iids
     * that have not been assigned to oids.
     */
    @Test
    public void testNullForMissingIid() {
        assertThat(entityIdManager.toIid(1L), is(1));
        assertThat(entityIdManager.toOidOrNull(1), is(1L));
        assertThat(entityIdManager.toOidOrNull(0), is(nullValue()));
        assertThat(entityIdManager.toOidOrNull(2), is(nullValue()));
    }

    /**
     * Test that the throwing iid-to-oid conversion method throws when given an iid less
     * than those any that have been assigned to oids.
     */
    @Test(expected = IndexOutOfBoundsException.class)
    public void testExceptionForLowMissingIid() {
        assertThat(entityIdManager.toIid(1L), is(1));
        assertThat(entityIdManager.toOidOrNull(1), is(1L));
        entityIdManager.toOid(0);
    }

    /**
     * Test that the throwing iid-to-oid conversion method throws when given an iid greater
     * than those any that have been assigned to oids.
     */
    @Test(expected = IndexOutOfBoundsException.class)
    public void testExceptionForHighMissingIid() {
        assertThat(entityIdManager.toIid(1L), is(1));
        assertThat(entityIdManager.toOidOrNull(1), is(1L));
        entityIdManager.toOid(2);
    }

    /**
     * Utility function to supply suitable long values for use as oids in {@link EntityIdManager}.
     *
     * <p>The class assumes that oids are nonzero, and the tests assume they are all distinct.</p>
     *
     * @return another suitable oid value
     */
    private long getRandomOid() {
        for (long next = rand.nextLong(); ; next = rand.nextLong()) {
            if (next != 0L && !repeatCheck.contains(next)) {
                return next;
            }
        }
    }
}

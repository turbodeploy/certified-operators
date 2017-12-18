package com.vmturbo.group.persistent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.persistent.TemporaryGroupCache.InvalidTempGroupException;

public class TemporaryGroupCacheTest {

    private IdentityProvider identityProvider = mock(IdentityProvider.class);

    private static final TempGroupInfo TEMP_GROUP_INFO = TempGroupInfo.newBuilder()
            .setName("the krew")
            .setEntityType(10)
            .build();

    private static final long GROUP_ID = 7L;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        when(identityProvider.next()).thenReturn(GROUP_ID);
    }

    @Test
    public void testCreateAndGet() throws InvalidTempGroupException {
        final TemporaryGroupCache temporaryGroupCache =
                new TemporaryGroupCache(identityProvider, 1, TimeUnit.MINUTES);
        final Group group = temporaryGroupCache.create(TEMP_GROUP_INFO);
        assertThat(group.getId(), is(GROUP_ID));
        assertThat(group.getType(), is(Type.TEMP_GROUP));
        assertThat(group.getOrigin(), is(Origin.USER));
        assertThat(group.getTempGroup(), is(TEMP_GROUP_INFO));

        final Optional<Group> getResult = temporaryGroupCache.get(GROUP_ID);
        assertThat(getResult.get(), is(group));
    }

    @Test
    public void testCreateValidationFailureNoName() throws InvalidTempGroupException {
        final TemporaryGroupCache temporaryGroupCache =
                new TemporaryGroupCache(identityProvider, 1, TimeUnit.MINUTES);
        expectedException.expect(InvalidTempGroupException.class);
        temporaryGroupCache.create(TempGroupInfo.newBuilder()
                .setEntityType(7)
                .build());
    }

    @Test
    public void testCreateValidationFailureNoEntityType() throws InvalidTempGroupException {
        final TemporaryGroupCache temporaryGroupCache =
                new TemporaryGroupCache(identityProvider, 1, TimeUnit.MINUTES);
        expectedException.expect(InvalidTempGroupException.class);
        temporaryGroupCache.create(TempGroupInfo.newBuilder()
                .setName("foo")
                .build());
    }

    @Test
    public void testExpire() throws InvalidTempGroupException, InterruptedException {
        final TemporaryGroupCache temporaryGroupCache =
                new TemporaryGroupCache(identityProvider, 1, TimeUnit.MICROSECONDS);
        final Group group = temporaryGroupCache.create(TEMP_GROUP_INFO);

        final long timeLimit = 30000; // 30 seconds
        final long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeLimit) {
            // The cache should perform some maintenance during reads - we shouldn't need
            // to have any writes or any other operations in order for the entries to expire.
            if (temporaryGroupCache.get(group.getId()).isPresent()) {
                Thread.sleep(10);
            } else {
                return;
            }
        }
        Assert.fail("Temporary group not expired after " + timeLimit + " ms");
    }

    @Test
    public void testDelete() throws InvalidTempGroupException {
        final TemporaryGroupCache temporaryGroupCache =
                new TemporaryGroupCache(identityProvider, 1, TimeUnit.MINUTES);
        final Group group = temporaryGroupCache.create(TEMP_GROUP_INFO);
        // Do the delete, make sure it returns the group
        assertThat(temporaryGroupCache.delete(group.getId()).get(), is(group));

        // Subsequent gets should fail.
        assertFalse(temporaryGroupCache.get(group.getId()).isPresent());
    }

    @Test
    public void testGetAll() throws InvalidTempGroupException {
        // Make sure the identity provider returns different IDs for the two groups.
        // This will always happen with a real identity provider.
        when(identityProvider.next()).thenReturn(7L, 8L);

        final TemporaryGroupCache temporaryGroupCache =
                new TemporaryGroupCache(identityProvider, 1, TimeUnit.MINUTES);

        final Group group1 = temporaryGroupCache.create(TEMP_GROUP_INFO);
        // The cache creates separate groups even if given the same temp group info.
        final Group group2 = temporaryGroupCache.create(TEMP_GROUP_INFO);
        assertThat(temporaryGroupCache.getAll(), containsInAnyOrder(group1, group2));
    }
}

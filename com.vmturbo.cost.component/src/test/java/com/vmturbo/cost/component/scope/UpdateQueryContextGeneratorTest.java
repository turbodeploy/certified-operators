package com.vmturbo.cost.component.scope;

import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.ALIAS_OID;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.BROADCAST_TIME_UTC_MS;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.BROADCAST_TIME_UTC_MS_NEXT_DAY;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.REAL_OID;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.REAL_OID_2;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.REAL_OID_3;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.createOidMapping;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.cost.component.scope.UpdateQueryContextGenerator.UpdateQueryContext;

/**
 * Unit tests for {@link UpdateQueryContextGenerator}.
 */
public class UpdateQueryContextGeneratorTest {

    /**
     * Test {@link UpdateQueryContext} data when there are no prior registrations for the provided alias oid for
     * a single new mapping.
     */
    @Test
    public void testGetUpdateQueryContextNoPriorReplacementSingleMapping() {
        final OidMapping oidMapping = createOidMapping(ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS);
        final UpdateQueryContextGenerator generator = new UpdateQueryContextGenerator(
            Collections.emptyMap(), Collections.singleton(oidMapping));
        final List<UpdateQueryContextGenerator.UpdateQueryContext> contexts = generator.getUpdateQueryContexts();
        Assert.assertFalse(contexts.isEmpty());
        verifyUpdateQueryContext(contexts.iterator().next(), oidMapping, null, null);
    }

    /**
     * Test {@link UpdateQueryContext} data when there are no prior registrations for the provided alias oid for
     * a two new mapping from different time windows.
     */
    @Test
    public void testGetUpdateQueryContextsNoPriorReplacementTwoMappingsDifferentWindows() {
        final OidMapping oidMapping1 = createOidMapping(ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS);
        final OidMapping oidMapping2 = createOidMapping(ALIAS_OID, REAL_OID_2, BROADCAST_TIME_UTC_MS_NEXT_DAY);
        final UpdateQueryContextGenerator generator = new UpdateQueryContextGenerator(
            Collections.emptyMap(), Arrays.asList(oidMapping1, oidMapping2));
        final List<UpdateQueryContext> contexts = generator.getUpdateQueryContexts();
        Assert.assertEquals(2, contexts.size());
        verifyUpdateQueryContext(contexts.stream().filter(u -> oidMapping1.equals(u.getCurrentOidMapping()))
            .findAny().orElse(null), oidMapping1, null, oidMapping2);
        verifyUpdateQueryContext(contexts.stream().filter(u -> oidMapping2.equals(u.getCurrentOidMapping()))
            .findAny().orElse(null), oidMapping2, null, null);
    }

    /**
     * Test {@link UpdateQueryContext} data when there are no prior registrations for the provided alias oid for
     * a two new mapping from same time windows.
     */
    @Test
    public void testGetUpdateQueryContextsNoPriorReplacementTwoMappingsSameWindows() {
        final OidMapping oidMapping1 = createOidMapping(ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS);
        final OidMapping oidMapping2 = createOidMapping(ALIAS_OID, REAL_OID_2, BROADCAST_TIME_UTC_MS
            + TimeUnit.HOURS.toMillis(1));
        final UpdateQueryContextGenerator generator = new UpdateQueryContextGenerator(
            Collections.emptyMap(), Arrays.asList(oidMapping1, oidMapping2));
        final List<UpdateQueryContext> contexts = generator.getUpdateQueryContexts();
        Assert.assertEquals(1, contexts.size());
        verifyUpdateQueryContext(contexts.stream().filter(u -> oidMapping2.equals(u.getCurrentOidMapping()))
            .findAny().orElse(null), oidMapping2, null, null);
    }

    /**
     * Test {@link UpdateQueryContext} data when there is one prior registration for the provided alias oid for
     * a single new mapping.
     */
    @Test
    public void testGetUpdateQueryContextsWithPriorReplacementOneMapping() {
        final OidMapping oidMapping1 = createOidMapping(ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS);
        final OidMapping oidMapping2 = createOidMapping(ALIAS_OID, REAL_OID_2, BROADCAST_TIME_UTC_MS_NEXT_DAY);
        final UpdateQueryContextGenerator generator = new UpdateQueryContextGenerator(
            Collections.singletonMap(ALIAS_OID, Collections.singleton(oidMapping1)),
            Collections.singleton(oidMapping2));
        final List<UpdateQueryContext> contexts = generator.getUpdateQueryContexts();
        Assert.assertEquals(1, contexts.size());
        verifyUpdateQueryContext(contexts.stream().filter(u -> oidMapping2.equals(u.getCurrentOidMapping()))
            .findAny().orElse(null), oidMapping2, oidMapping1, null);
    }

    /**
     * Test {@link UpdateQueryContext} data when there are two prior registrations for the provided alias oid for
     * a single new mapping.
     */
    @Test
    public void testGetUpdateQueryContextWithTwoPriorReplacementOneMappings() {
        final OidMapping oidMapping1 = createOidMapping(ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS);
        final OidMapping oidMapping2 = createOidMapping(ALIAS_OID, REAL_OID_2, BROADCAST_TIME_UTC_MS_NEXT_DAY);
        final OidMapping oidMapping3 = createOidMapping(ALIAS_OID, REAL_OID_3, BROADCAST_TIME_UTC_MS_NEXT_DAY
            + TimeUnit.DAYS.toMillis(1));
        final UpdateQueryContextGenerator generator = new UpdateQueryContextGenerator(
            Collections.singletonMap(ALIAS_OID, new HashSet<>(Arrays.asList(oidMapping2, oidMapping1))),
            Collections.singleton(oidMapping3));
        final List<UpdateQueryContext> contexts = generator.getUpdateQueryContexts();
        Assert.assertEquals(1, contexts.size());
        verifyUpdateQueryContext(contexts.stream().filter(u -> oidMapping3.equals(u.getCurrentOidMapping()))
            .findAny().orElse(null), oidMapping3, oidMapping2, null);
    }

    /**
     * Test {@link UpdateQueryContext} data when there is one prior registration for the provided alias oid for
     * a single new mapping.
     */
    @Test
    public void testGetUpdateQueryContextWithOnePriorReplacementTwoMappings() {
        final OidMapping oidMapping1 = createOidMapping(ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS);
        final OidMapping oidMapping2 = createOidMapping(ALIAS_OID, REAL_OID_2, BROADCAST_TIME_UTC_MS_NEXT_DAY);
        final OidMapping oidMapping3 = createOidMapping(ALIAS_OID, REAL_OID_3, BROADCAST_TIME_UTC_MS_NEXT_DAY
            + TimeUnit.DAYS.toMillis(1));
        final UpdateQueryContextGenerator generator = new UpdateQueryContextGenerator(
            Collections.singletonMap(ALIAS_OID, new HashSet<>(Collections.singletonList(oidMapping1))),
            Arrays.asList(oidMapping3, oidMapping2));
        final List<UpdateQueryContext> contexts = generator.getUpdateQueryContexts();
        Assert.assertEquals(2, contexts.size());
        verifyUpdateQueryContext(contexts.stream().filter(u -> oidMapping2.equals(u.getCurrentOidMapping()))
            .findAny().orElse(null), oidMapping2, oidMapping1, oidMapping3);
        verifyUpdateQueryContext(contexts.stream().filter(u -> oidMapping3.equals(u.getCurrentOidMapping()))
            .findAny().orElse(null), oidMapping3, oidMapping1, null);
    }

    private void verifyUpdateQueryContext(final UpdateQueryContext updateQueryContext,
                                          final OidMapping expectedMappingUnderConsideration,
                                          final OidMapping mostRecentRegisteredReplacementForAliasOid,
                                          final OidMapping nextReplacement) {
        Assert.assertNotNull(updateQueryContext);
        Assert.assertEquals(expectedMappingUnderConsideration, updateQueryContext.getCurrentOidMapping());
        Assert.assertEquals(Optional.ofNullable(mostRecentRegisteredReplacementForAliasOid),
            updateQueryContext.getLastReplacedOidMappingForAlias());
        Assert.assertEquals(Optional.ofNullable(nextReplacement), updateQueryContext.getNextOidMappingForAlias());
    }
}
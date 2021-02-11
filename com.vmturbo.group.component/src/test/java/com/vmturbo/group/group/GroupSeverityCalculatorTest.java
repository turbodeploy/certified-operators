package com.vmturbo.group.group;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.action.orchestrator.api.EntitySeverityClientCache;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;

/**
 * Unit tests for {@link GroupSeverityCalculator}.
 */
public class GroupSeverityCalculatorTest {

    private final EntitySeverityClientCache entitySeverityCache =
            mock(EntitySeverityClientCache.class);

    private final GroupSeverityCalculator calculator =
            new GroupSeverityCalculator(entitySeverityCache);

    /**
     * Adds severities to the cache.
     *
     * @param severities a list of severities to add to the cache. They will be assigned to
     *                   incrementing entity oids beginning from 1.
     */
    private void prepareSeverities(List<Severity> severities) {
        long currentOid = 1;
        for (Severity s : severities) {
            when(entitySeverityCache.getEntitySeverity(currentOid)).thenReturn(s);
            currentOid++;
        }
    }

    /**
     * Tests that in a group where all entities have NORMAL severity, NORMAL is returned as the
     * group's severity.
     */
    @Test
    public void testNormalSeverity() {
        // GIVEN
        List<Severity> severities = Arrays.asList(Severity.NORMAL, Severity.NORMAL, Severity.NORMAL);
        prepareSeverities(severities);
        // WHEN
        Severity groupSeverity = calculator.calculateSeverity(Sets.newHashSet(1L, 2L, 3L));
        //THEN
        Assert.assertEquals(Severity.NORMAL, groupSeverity);
    }

    /**
     * Tests that in a group where highest entity severity is MINOR, MINOR is returned as the
     * group's severity.
     */
    @Test
    public void testMinorSeverity() {
        // GIVEN
        List<Severity> severities = Arrays.asList(Severity.MINOR, Severity.NORMAL, Severity.NORMAL);
        prepareSeverities(severities);
        // WHEN
        Severity groupSeverity = calculator.calculateSeverity(Sets.newHashSet(1L, 2L, 3L));
        //THEN
        Assert.assertEquals(Severity.MINOR, groupSeverity);
    }

    /**
     * Tests that in a group where highest entity severity is MAJOR, MAJOR is returned as the
     * group's severity.
     */
    @Test
    public void testMajorSeverity() {
        // GIVEN
        List<Severity> severities = Arrays.asList(Severity.NORMAL, Severity.MAJOR, Severity.MINOR);
        prepareSeverities(severities);
        // WHEN
        Severity groupSeverity = calculator.calculateSeverity(Sets.newHashSet(1L, 2L, 3L));
        //THEN
        Assert.assertEquals(Severity.MAJOR, groupSeverity);
    }

    /**
     * Tests that in a group where highest entity severity is CRITICAL, CRITICAL is returned as the
     * group's severity.
     */
    @Test
    public void testCriticalSeverity() {
        // GIVEN
        List<Severity> severities = Arrays.asList(Severity.CRITICAL, Severity.NORMAL, Severity.MINOR);
        prepareSeverities(severities);
        // WHEN
        Severity groupSeverity = calculator.calculateSeverity(Sets.newHashSet(1L, 2L, 3L));
        //THEN
        Assert.assertEquals(Severity.CRITICAL, groupSeverity);
    }

    /**
     * Tests that NORMAL is returned as the severity of an empty group.
     */
    @Test
    public void testSeverityForEmptyGroup() {
        // WHEN
        Severity groupSeverity = calculator.calculateSeverity(Collections.emptySet());
        //THEN
        Assert.assertEquals(Severity.NORMAL, groupSeverity);
    }
}

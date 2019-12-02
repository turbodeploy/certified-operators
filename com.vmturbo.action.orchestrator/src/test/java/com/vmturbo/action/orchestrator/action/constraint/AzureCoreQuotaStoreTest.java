package com.vmturbo.action.orchestrator.action.constraint;

import static com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils.buildCoreQuotaActionConstraintInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.Arrays;

import org.junit.Test;

import com.vmturbo.components.common.utils.StringConstants;

/**
 * Tests for {@link CoreQuotaStore}.
 */
public class AzureCoreQuotaStoreTest {

    /**
     * Test {@link CoreQuotaStore#getCoreQuotaStore()}.
     * This method should always return the same object.
     */
    @Test
    public void testGetAzureCoreQuotaStore() {
        assertSame(CoreQuotaStore.getCoreQuotaStore(),
            CoreQuotaStore.getCoreQuotaStore());
    }

    /**
     * Test {@link CoreQuotaStore#getCoreQuota(long, long, String)}.
     */
    @Test
    public void testGetAzureCoreQuota() {
        final long businessAccountId1 = 1;
        final long businessAccountId2 = 2;
        final long regionId1 = 11;
        final long regionId2 = 12;
        final String family1 = "family1";
        final String family2 = "family2";
        final int value = 100;

        CoreQuotaStore coreQuotaStore = CoreQuotaStore.getCoreQuotaStore();
        coreQuotaStore.updateActionConstraintInfo(buildCoreQuotaActionConstraintInfo(
            Arrays.asList(businessAccountId1, businessAccountId2), Arrays.asList(regionId1, regionId2),
            Arrays.asList(family1, family2), value));

        for (long businessAccountId : Arrays.asList(businessAccountId1, businessAccountId2)) {
            for (long regionId : Arrays.asList(regionId1, regionId2)) {
                for (String family : Arrays.asList(family1, family2)) {
                    assertEquals(value, coreQuotaStore.getCoreQuota(
                        businessAccountId, regionId, family));
                }
                assertEquals(value, coreQuotaStore.getCoreQuota(
                    businessAccountId, regionId, StringConstants.TOTAL_CORE_QUOTA));
            }
        }

        // Azure core quota not found.
        assertEquals(Integer.MAX_VALUE, coreQuotaStore.getCoreQuota(3, regionId1, family1));
        assertEquals(Integer.MAX_VALUE, coreQuotaStore.getCoreQuota(businessAccountId1, 3, family1));
        assertEquals(Integer.MAX_VALUE, coreQuotaStore.getCoreQuota(businessAccountId1, regionId1, "3"));
    }
}

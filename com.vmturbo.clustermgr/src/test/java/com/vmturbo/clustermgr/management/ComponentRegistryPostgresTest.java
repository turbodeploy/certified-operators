package com.vmturbo.clustermgr.management;

import org.junit.ClassRule;
import org.junit.Rule;
import org.springframework.test.context.TestPropertySource;

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Test for the case that {@link FeatureFlags#POSTGRES_PRIMARY_DB} is enabled and POSTGRES
 * is used as the dialect.
 */
@TestPropertySource(properties = {"sqlDialect=POSTGRES"})
public class ComponentRegistryPostgresTest extends ComponentRegistryTest {

    /**
     * Prevent mysql setup by hiding the existing rule with same field name.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = null;

    /**
     * Prevent mysql cleanup by hiding the existing rule with same field name.
     */
    @Rule
    public DbCleanupRule dbCleanup = null;

    /**
     * FF enabled by default.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule(
            FeatureFlags.POSTGRES_PRIMARY_DB);
}

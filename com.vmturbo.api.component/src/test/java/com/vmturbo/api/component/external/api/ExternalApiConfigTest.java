package com.vmturbo.api.component.external.api;

import static com.vmturbo.api.component.external.api.HeaderApiSecurityConfig.VENDOR_URL_CONTEXT_TAG;
import static com.vmturbo.api.component.security.HeaderAuthenticationCondition.ENABLED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

/**
 * Verify {@link ExternalApiConfig}.
 */
@NotThreadSafe // must not be run in parallel
public class ExternalApiConfigTest {

    /**
     * Save System Properties before each test, and restore afterwards.
     */
    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    /**
     * Verify adding correct context URL in integration mode (HeaderAuthentication is enabled).
     */
    @Test
    public void testGetBaseURLMappingsAddingVendorContextUrl() {
        environmentVariables.set(ENABLED, "true");
        environmentVariables.set(VENDOR_URL_CONTEXT_TAG, "wo");
        assertThat(ExternalApiConfig.getBaseURLMappings(), hasItem("/api/wo/v3/*"));
    }
}
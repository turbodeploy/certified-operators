package com.vmturbo.components.common.config;

import static com.vmturbo.components.common.config.SensitiveDataUtil.getSensitiveKey;
import static com.vmturbo.components.common.config.SensitiveDataUtil.hasSensitiveData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

/**
 * Verify {@link SensitiveDataUtil}.
 */
public class SensitiveDataUtilTest {

    /**
     * Test has sensitive data check.
     */
    @Test
    public void testHasSensitiveData() {
        getSensitiveKey().forEach(e -> assertTrue(hasSensitiveData(e)));
    }

    /**
     * Test get sensitive keys.
     */
    @Test
    public void testGetSensitiveKey() {
        Set<String> sensitiveKeySet =
                ImmutableSet.of("arangodbPass", "userPassword", "sslKeystorePassword",
                        "readonlyPassword", "dbRootPassword", "actionDbPassword", "authDbPassword",
                        "costDbPassword", "groupComponentDbPassword", "historyDbPassword",
                        "clustermgrDbPassword", "planDbPassword", "topologyProcessorDbPassword",
                        "repositoryDbPassword", "intersightClientSecret");
        assertEquals(sensitiveKeySet, getSensitiveKey());
    }
}
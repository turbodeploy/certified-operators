package com.vmturbo.api.component.security;

import static com.vmturbo.api.component.security.IntersightIdTokenVerifierTest.PUBLIC_KEY_WITH_PREFIX_SUFFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.security.PublicKey;
import java.util.Collections;
import java.util.Optional;

import org.junit.Test;

/**
 * Tests for {@link IntersightHeaderMapper}.
 */
public class IntersightHeaderMapperTest {

    /**
     * Test build RSA based public key from passed in text based key.
     */
    @Test
    public void testBuildIntersightPublicKey() {
        HeaderMapper mapper = new IntersightHeaderMapper(Collections.emptyMap(), "", "", "", "");
        Optional<PublicKey> publicKey =
                mapper.buildPublicKey(Optional.of(PUBLIC_KEY_WITH_PREFIX_SUFFIX));
        assertTrue(publicKey.isPresent());
        assertEquals("RSA", publicKey.get().getAlgorithm());
    }
}
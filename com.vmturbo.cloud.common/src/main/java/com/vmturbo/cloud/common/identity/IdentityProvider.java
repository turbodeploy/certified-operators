package com.vmturbo.cloud.common.identity;

import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * The identity provider interface.
 */
public interface IdentityProvider {

    /**
     * Returns the next available ID.
     * @return The next available ID.
     */
    long next();

    /**
     * A default implementation of {@link IdentityProvider}, based on {@link IdentityGenerator}.
     */
    class DefaultIdentityProvider implements IdentityProvider {

        /**
         * Constructs a new {@link DefaultIdentityProvider}.
         * @param identityGeneratorPrefix The identity generator prefix, passed to {@link IdentityGenerator}.
         */
        public DefaultIdentityProvider(final long identityGeneratorPrefix) {
            IdentityGenerator.initPrefix(identityGeneratorPrefix);
        }

        /**
         * {@inheritDoc}.
         */
        @Override
        public long next() {
            return IdentityGenerator.next();
        }
    }
}

package com.vmturbo.group.policy;

import java.util.Map;

import javax.annotation.Nonnull;

/**
 * A factory for {@link DiscoveredPoliciesMapper} instances, mainly used for dependency injection
 * in tests.
 */
public interface DiscoveredPoliciesMapperFactory {

    @Nonnull
    DiscoveredPoliciesMapper newMapper(@Nonnull final Map<String, Long> groupIdsByName);

    /**
     * The default implementation of {@link DiscoveredPoliciesMapperFactory} for use in production.
     */
    class DefaultDiscoveredPoliciesMapperFactory implements  DiscoveredPoliciesMapperFactory {

        @Nonnull
        @Override
        public DiscoveredPoliciesMapper newMapper(@Nonnull final Map<String, Long> groupIdsByName) {
            return new DiscoveredPoliciesMapper(groupIdsByName);
        }
    }
}

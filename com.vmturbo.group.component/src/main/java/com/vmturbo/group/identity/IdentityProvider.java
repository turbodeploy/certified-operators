package com.vmturbo.group.identity;

import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * A wrapper for {@link IdentityGenerator}, so it is more Spring friendly.
 *
 * The methods exposed by this class mirror the methods in {@link IdentityGenerator}.
 * However, not all methods are exposed.
 */
public class IdentityProvider {
    public IdentityProvider(final long identityGeneratorPrefix) {
        IdentityGenerator.initPrefix(identityGeneratorPrefix);
    }

    public long next() {
        return IdentityGenerator.next();
    }

    public long toMilliTime(final long id) {
        return IdentityGenerator.toMilliTime(id);
    }
}

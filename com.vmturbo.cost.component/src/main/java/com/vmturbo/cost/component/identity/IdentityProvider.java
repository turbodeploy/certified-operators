package com.vmturbo.cost.component.identity;

import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * A wrapper for {@link IdentityGenerator}.
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


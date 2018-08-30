package com.vmturbo.identity.attributes;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.identity.store.IdentityStoreException;

/**
 * Capture a {@link Set} of {@link IdentityMatchingAttribute} values. Used as a key during the
 * generation of OIDs for items of a given type. Items whose {@link IdentityMatchingAttributes}
 * are equal are considered to be the same object, and hence are represented by the same OID.
 * Note that this will likely require the implementor of this interface to provide their own
 * 'equals()' and 'hashCode()' implementations as well.
 */
public interface IdentityMatchingAttributes {
    /**
     * Return the Set of IdentityMatchingAttribute objects for this instance. Each
     * IdentityMatchingAttribute consists of an attributeId and attributeValue.
     */
    @Nonnull
    Set<IdentityMatchingAttribute> getMatchingAttributes();

    @Nonnull
    IdentityMatchingAttribute getMatchingAttribute(String attributeId)
            throws IdentityStoreException;
}

package com.vmturbo.identity;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.identity.exceptions.IdentityServiceException;

/**
 * Identity service is responsible for assigning OIDs to some business objects. OIDs could be
 * stable across the restarts depending on implementation. OIDs will be reused within lifecycle
 * of an identity store
 *
 * @param <T> business objects type to assign OIDs to.
 */
public interface IdentityService<T> {
    /**
     * Fetches or assigns a new OIDs to all the business objects specified in the parameters.
     * Implementation must guarantee that resulting list has the same length as an input list
     * and order is preserved. I.e.
     * {@code businessObjects.get(i)} will be associated with OID {@code result.get(i)}
     *
     * @param businessObjects objects to assign or fetch OIDs to
     * @return list of OIDs for the specified objects
     * @throws NullPointerException if {@code businessObjects} passed is {@code null}
     * @throws IdentityServiceException if exception occurred while retrieving (or
     *         assigning) OIDs
     */
    @Nonnull
    List<Long> getOidsForObjects(@Nonnull List<T> businessObjects) throws IdentityServiceException;
}

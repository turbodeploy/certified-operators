package com.vmturbo.api.component.security;

import java.security.PublicKey;
import java.util.Optional;

import javax.annotation.Nullable;

/**
 * A interface to map headers from external requests to XL, e.g.:
 * <ul>
 * <li> barracuda_account -> user name
 * <li> barracuda_roles -> user role -> external user group
 * e.g. Service Administrator -> Administrator -> administrator (group)
 * </ul>
 */
public interface HeaderMapper {
    /**
     * Retrieve the property name that will have JWT token public key as value.
     *
     * @return the vender's JWT token public key property name.
     */
    String getJwtTokenPublicKeyTag();

    /**
     * Get external vendor username.
     *
     * @return external vendor username.
     */
    String getUserName();

    /**
     * Get external vendeor role.
     *
     * @return external vendeor role.
     */
    String getRole();

    /**
     * Get XL role.
     *
     * @param externalRole external vendor role
     * @return get xl role.
     */
    String getAuthGroup(@Nullable String externalRole);

    /**
     * Vendor dependent implementation to build {@link PublicKey} to verify vendor's JWT token. It could be previous
     * supported version.
     *
     * @param jwtTokenPublicKey the vendor's public key in text format.
     * @return the public key in {@link PublicKey} format.
     */
    Optional<PublicKey> buildPublicKey(Optional<String> jwtTokenPublicKey);

    /**
     * Vendor dependent implementation to build latest {@link PublicKey} to verify vendor's JWT token.
     *
     * @param jwtTokenPublicKey the vendor's public key in text format.
     * @return the public key in {@link PublicKey} format.
     */
    Optional<PublicKey> buildPublicKeyLatest(Optional<String> jwtTokenPublicKey);

    /**
     * Retrieve the property name that will have JWT token as value.
     *
     * @return the vender's JWT token property name.
     */
    String getJwtTokenTag();
}

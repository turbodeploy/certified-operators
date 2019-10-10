package com.vmturbo.reserved.instance.coverage.allocator.key;

/**
 * Defines constants to be used as keys in {@link CoverageKey#getKeyMaterial()}.
 */
public interface CoverageKeyMaterial {

    /**
     * The region OID
     */
    String REGION_OID_KEY = "REGION_OID";

    /**
     * The availability zone OID. This key is only used if {@link CoverageKeyCreationConfig#isZoneScoped()}
     * is true.
     */
    String AVAILABILITY_ZONE_OID_KEY = "AVAILABILITY_ZONE_OID";

    /**
     * The family of a tier connected to an entity or RI
     */
    String TIER_FAMILY_KEY = "TIER_FAMILY";

    /**
     * The OID of a tier connected to an entity or RI. This key is only used if
     * {@link CoverageKeyCreationConfig#isInstanceSizeFlexible()} is false.
     */
    String TIER_OID_KEY = "TIER_OID";

    /**
     * Represents the tenancy of an RI or entity
     */
    String TENANCY_KEY = "TENANCY";

    /**
     * Represents the platform/OS of an RI or entity
     */
    String PLATFORM_KEY = "PLATFORM";

    /**
     * The OID of a BusinessAccount (either the master or direct owner account), depending on
     * {@link CoverageKeyCreationConfig#isSharedScope()}
     */
    String ACCOUNT_SCOPE_OID_KEY = "ACCOUNT_SCOPE_OID";
}

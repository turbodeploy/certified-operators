package com.vmturbo.cloud.commitment.analysis.spec;

import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * A key used to identify the scope of a {@link ReservedInstanceSpec}. This key helps to identify
 * when an RI spec can cover a demand cluster (e.g. when the demand cluster's region OID matches
 * the region OID of an RI spec key for an RI spec).
 */
@Value.Immutable
public interface ReservedInstanceSpecKey {

    /**
     * The region OID of the key.
     *
     * @return The region OID of the key.
     */
    long regionOid();

    /**
     * The computer tier OID of the key. This value will be null when the associated set.
     *
     * @return The computer tier OID of the key. This value will be null when the associated set
     * of RI specs are instance size flexible.
     */
    @Nullable
    Long computerTierOid();

    /**
     * The family of the associated compute tier for this key.
     *
     * @return The family of the associated compute tier for this key.
     */
    String family();

    /**
     * The OS of the key. This value will be null when the target set of RI specs.
     *
     * @return The OS of the key. This value will be null when the target set of RI specs
     * are platform-flexible.
     */
    @Nullable
    OSType osType();

    /**
     * The tenancy of the key.
     *
     * @return The tenancy of the key.
     */
    Tenancy tenancy();

}

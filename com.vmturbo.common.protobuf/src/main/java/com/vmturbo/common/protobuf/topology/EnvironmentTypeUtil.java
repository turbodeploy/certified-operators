package com.vmturbo.common.protobuf.topology;

import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Utility methods for environment types.
 */
public class EnvironmentTypeUtil {
    /**
     * Entities discovered by these probes should be considered CLOUD entities.
     */
    public static final Set<SDKProbeType> CLOUD_PROBE_TYPES = ImmutableSet.of(
        SDKProbeType.AWS,
        SDKProbeType.AWS_COST,
        SDKProbeType.AWS_BILLING,
        SDKProbeType.AZURE,
        SDKProbeType.AZURE_COST,
        SDKProbeType.AZURE_EA,
        SDKProbeType.AZURE_INFRA,
        SDKProbeType.AZURE_SERVICE_PRINCIPAL,
        SDKProbeType.AZURE_STORAGE_BROWSE,
        SDKProbeType.GCP_SERVICE_ACCOUNT,
        SDKProbeType.GCP_COST,
        SDKProbeType.GCP_BILLING,
        SDKProbeType.GCP_PROJECT,
        SDKProbeType.AZURE_BILLING,
        SDKProbeType.AZURE_PRICING);

    /**
     * Mapping between {@link EnvironmentType} enum values and their API
     * string representation.
     */
    private static final BiMap<EnvironmentType, String> ENV_TYPE_MAPPINGS =
            new ImmutableBiMap.Builder<EnvironmentType, String>()
                    .put(EnvironmentType.HYBRID, "HYBRID")
                    .put(EnvironmentType.CLOUD, "CLOUD")
                    .put(EnvironmentType.ON_PREM, "ONPREM")
                    .put(EnvironmentType.UNKNOWN_ENV, "UNKNOWN")
                    .build();

    private EnvironmentTypeUtil() {}

    /**
     * Decides if an environment type value matches an environment type query.
     *
     * <p>In particular:
     *   <ul>
     *       <li>if the query value is {@link EnvironmentType#HYBRID} or {@code null},
     *           then all environment types match</li>
     *       <li>if the query value is {@link EnvironmentType#CLOUD}, then
     *           environment types {@link EnvironmentType#HYBRID} and
     *           {@link EnvironmentType#CLOUD} match</li>
     *       <li>if the query value is {@link EnvironmentType#ON_PREM}, then
     *           environment types {@link EnvironmentType#HYBRID} and
     *           {@link EnvironmentType#ON_PREM} match</li>
     *       <li>if the query value is {@link EnvironmentType#UNKNOWN_ENV},
     *           then only environment type {@link EnvironmentType#UNKNOWN_ENV}
     *           and the {@code null} environment type match</li>
     *   </ul>
     * </p>
     *
     * @param query environment type to be matched against
     * @param value environment type to match
     * @return true if and only if the query and the value match
     *         according to the above rules
     */
    public static boolean match(@Nullable EnvironmentType query, @Nullable EnvironmentType value) {
        if (query == null || query == EnvironmentType.HYBRID || query == value) {
            // if the query does not filter or query matches the value exactly
            // return true
            return true;
        } else if (query == EnvironmentType.UNKNOWN_ENV) {
            // if the query is equal to UNKNOWN_ENV, then it also matches a missing value
            return value == null;
        } else {
            // if the query is either CLOUD or ON_PREM, then it also matches hybrid
            return value == EnvironmentType.HYBRID;
        }
    }

    /**
     * Given a query environment type, returns a matching predicate that behaves
     * on a value environment type according to the rules of
     * {@link EnvironmentTypeUtil#match}. This method is used to optimize filtering
     * of streams of entities, when the filtering environment type is known in
     * advance.
     *
     * @param query the query environment type
     * @return predicate that accepts or rejects a value environment type
     *         according to the rules of {@link EnvironmentTypeUtil#match}
     */
    @Nonnull
    public static Predicate<EnvironmentType> matchingPredicate(@Nullable EnvironmentType query) {
        if (query == null || query == EnvironmentType.HYBRID) {
            return e -> true;
        } else if (query == EnvironmentType.UNKNOWN_ENV) {
            return e -> e == query || e == null;
        } else {
            return e -> e == query || e == EnvironmentType.HYBRID;
        }
    }

    /**
     * Translates an API string to an {@link EnvironmentType} object.
     *
     * <p>Normally, this method should be defined only in the API component.
     *    Unfortunately, API string representations of environment types are
     *    used in search filters (which are communicated between the API
     *    component and the Repository and Group components and stored in
     *    the groups database) and they are stored in ArangoDB topologies.
     *    TODO: retire these usages, esp. searching with string filters
     *          and then get rid of this method completely.
     * </p>
     *
     * @param apiString API string representation of an environment type
     * @return the corresponding environment type, if any
     */
    @Nonnull
    public static Optional<EnvironmentType> fromApiString(@Nullable String apiString) {
        if (apiString == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(ENV_TYPE_MAPPINGS.inverse().get(apiString));
    }

    /**
     * Translates an {@link EnvironmentType} object to the string that
     * represents it in the API.
     *
     * <p>Normally, this method should be defined only in the API component.
     *    Unfortunately, API string representations of environment types are
     *    used in search filters (which are communicated between the API
     *    component and the Repository and Group components and stored in
     *    the groups database) and they are stored in ArangoDB topologies.
     *    TODO: retire these usages, esp. searching with string filters
     *          and then get rid of this method completely.
     * </p>
     *
     * @param environmentType the environment type
     * @return the corresponding API string representation
     */
    @Nonnull
    public static String toApiString(@Nonnull EnvironmentType environmentType) {
        return ENV_TYPE_MAPPINGS.get(environmentType);
    }
}

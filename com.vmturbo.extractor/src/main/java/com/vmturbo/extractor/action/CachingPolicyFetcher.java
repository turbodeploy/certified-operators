package com.vmturbo.extractor.action;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import io.grpc.StatusRuntimeException;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;

/**
 * Responsible for fetching policies required for action descriptions from the group component.
 * Caches them to avoid excessive queries when processing lots of small batches of completed
 * actions.
 */
@ThreadSafe
public class CachingPolicyFetcher {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyServiceBlockingStub policyService;

    private final long updateIntervalMillis;

    private final MutableLong lastPolicyUpdate = new MutableLong(0);

    /**
     * Cached policy map. It's used by both pending actions and executed actions. To ensure we have
     * latest policy info for executed actions, we need to update it regularly (10 min for now).
     */
    private volatile Map<Long, Policy> policyById = Collections.emptyMap();

    private final Clock clock;

    CachingPolicyFetcher(@Nonnull final PolicyServiceBlockingStub policyService,
            @Nonnull final Clock clock,
            final long updateInterval,
            @Nonnull final TimeUnit updateIntervalUnit) {
        this.policyService = policyService;
        this.clock = clock;
        this.updateIntervalMillis = updateIntervalUnit.toMillis(updateInterval);
    }

    /**
     * Get latest policy map or fetch from group component if it expires.
     *
     * @return map of policy by id
     */
    @Nonnull
    public synchronized Map<Long, Policy> getOrFetchPolicies() {
        final long now = clock.millis();
        final long nextUpdate = lastPolicyUpdate.longValue() + updateIntervalMillis;
        if (nextUpdate <= now) {
            try {
                final Map<Long, Policy> newPolicies = new HashMap<>();
                policyService.getPolicies(PolicyDTO.PolicyRequest.newBuilder().build())
                        .forEachRemaining(
                                response -> newPolicies.put(response.getPolicy().getId(),
                                        response.getPolicy()));
                logger.info("Retrieved {} policies from group component",
                        newPolicies.size());
                policyById = Collections.unmodifiableMap(newPolicies);
                lastPolicyUpdate.setValue(now);
            } catch (StatusRuntimeException e) {
                logger.error("Failed to fetch policies", e);
            }
        }
        return policyById;
    }
}

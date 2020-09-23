package com.vmturbo.common.protobuf.severity;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeveritiesResponse;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.EntitySeverity;
import com.vmturbo.common.protobuf.action.EntitySeverityDTO.MultiEntityRequest;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceStub;

/**
 * Util for fetching severities from AO and calculate severities for group of entities.
 */
public class SeverityUtil {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Private constructor.
     */
    private SeverityUtil() {
    }

    /**
     * Calculate the severities for the given entities.
     *
     * @param topologyContextId The ID of the topology context from which to retrieve the severity.
     * @param entityOids        The set of entity OIDs.
     * @param severityService   service for fetching entity severity
     * @return a mapping between entity id and corresponding severity.
     */
    @Nonnull
    public static Future<SeverityMap> calculateSeverities(final long topologyContextId,
            @Nonnull final Collection<Long> entityOids,
            @Nonnull final EntitySeverityServiceStub severityService) {
        final SeverityObserver observer = new SeverityObserver(entityOids.size());
        severityService.getEntitySeverities(MultiEntityRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .addAllEntityIds(entityOids)
                .build(), observer);
        return observer.getFuture();
    }

    /**
     * Local helper class used to store severity data retrieved from severity service.
     */
    public static class SeverityObserver implements StreamObserver<EntitySeveritiesResponse> {
        private final Map<Long, EntitySeverity> resultMap;
        private final CompletableFuture<SeverityMap> future;

        public SeverityObserver(int expectedSize) {
            this.resultMap = new HashMap<>(expectedSize);
            this.future = new CompletableFuture<>();
        }

        public CompletableFuture<SeverityMap> getFuture() {
            return future;
        }

        @Override
        public void onNext(EntitySeveritiesResponse value) {
            for (EntitySeverity severity : value.getEntitySeverity().getEntitySeverityList()) {
                if (resultMap.put(severity.getEntityId(), severity) != null) {
                    logger.warn("Received multiple records from severity service for entity {}",
                            severity.getEntityId());
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            future.complete(new SeverityMapper(Collections.unmodifiableMap(resultMap)));
        }
    }
}

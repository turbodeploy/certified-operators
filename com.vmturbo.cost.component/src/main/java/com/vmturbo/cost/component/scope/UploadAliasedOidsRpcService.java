package com.vmturbo.cost.component.scope;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.AliasedOidsServiceGrpc;
import com.vmturbo.common.protobuf.cost.AliasedOidsServices.UploadAliasedOidsRequest;
import com.vmturbo.common.protobuf.cost.AliasedOidsServices.UploadAliasedOidsResponse;

/**
 * RPC service implementation for {@link AliasedOidsServiceGrpc.AliasedOidsServiceImplBase}.
 */
public class UploadAliasedOidsRpcService extends AliasedOidsServiceGrpc.AliasedOidsServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final SqlOidMappingStore sqlOidMappingStore;
    private final List<ScopeIdReplacementLog> scopeIdReplacementLogs;

    /**
     * Create an instance of {@link UploadAliasedOidsRpcService}.
     *
     * @param sqlOidMappingStore instance to store the data.
     * @param scopeIdReplacementLogs instances for which scope id replacement must be persisted.
     */
    public UploadAliasedOidsRpcService(@Nonnull final SqlOidMappingStore sqlOidMappingStore,
                                       @Nonnull final List<ScopeIdReplacementLog> scopeIdReplacementLogs) {
        this.sqlOidMappingStore = Objects.requireNonNull(sqlOidMappingStore);
        this.scopeIdReplacementLogs = Objects.requireNonNull(scopeIdReplacementLogs);
    }

    @Override
    public void uploadAliasedOids(@Nonnull final UploadAliasedOidsRequest request,
                                  @Nonnull final StreamObserver<UploadAliasedOidsResponse> responseObserver) {
        Objects.requireNonNull(responseObserver);
            // persist new oid mappings
            if (Objects.requireNonNull(request).hasBroadcastTimeUtcMs() && request.getAliasIdToRealIdCount() > 0) {
                final long broadcastTimeUtcMs = request.getBroadcastTimeUtcMs();
                logger.info("Received {} mappings for broadcast time {}.", request.getAliasIdToRealIdCount(),
                    broadcastTimeUtcMs);
                final Collection<OidMapping> oidMappings = request.getAliasIdToRealIdMap().entrySet().stream()
                    .map(entry -> createOidMapping(entry.getKey(), entry.getValue(), broadcastTimeUtcMs))
                    .collect(Collectors.toSet());
                try {
                    sqlOidMappingStore.registerOidMappings(oidMappings);
                } catch (Exception ex) {
                    logger.error("Exception on registering oid mappings", ex);
                }
            } else {
                if (!request.hasBroadcastTimeUtcMs()) {
                    logger.error("UploadAliasedOidsRequest has no broadcast time set");
                }
            }

            // persist scope id replacements
            for (final ScopeIdReplacementLog scopeIdReplacementLog : scopeIdReplacementLogs) {
                try {
                    logger.info("Starting scope id replacements for {}, log id {}",
                        scopeIdReplacementLog.getClass().getSimpleName(), scopeIdReplacementLog.getLogId());
                    final Optional<ScopeIdReplacementPersistenceSummary> summary = scopeIdReplacementLog
                        .persistScopeIdReplacements();
                    logger.info("Scope id replacement summary for: {}, {}",
                        scopeIdReplacementLog.getClass().getSimpleName(), summary.isPresent() ? summary : "<empty>");
                } catch (Exception ex) {
                    logger.error("Failed to persist scope ids for {}", scopeIdReplacementLog.getClass().getSimpleName(),
                        ex);
                }
            }

        responseObserver.onNext(UploadAliasedOidsResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    private OidMapping createOidMapping(final long aliasOid, final long realOid, final long broadcastTimeUtcMs) {
        return ImmutableOidMapping.builder()
            .oidMappingKey(ImmutableOidMappingKey.builder()
                .aliasOid(aliasOid)
                .realOid(realOid)
                .build())
            .firstDiscoveredTimeMsUtc(Instant.ofEpochMilli(broadcastTimeUtcMs))
            .build();
    }
}
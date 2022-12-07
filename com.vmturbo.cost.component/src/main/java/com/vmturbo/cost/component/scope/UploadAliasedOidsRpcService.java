package com.vmturbo.cost.component.scope;

import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
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

    /**
     * Create an instance of {@link UploadAliasedOidsRpcService}.
     *
     * @param sqlOidMappingStore instance to store the data.
     */
    public UploadAliasedOidsRpcService(@Nonnull final SqlOidMappingStore sqlOidMappingStore) {
        this.sqlOidMappingStore = Objects.requireNonNull(sqlOidMappingStore);
    }

    @Override
    public void uploadAliasedOids(@Nonnull final UploadAliasedOidsRequest request,
                                  @Nonnull final StreamObserver<UploadAliasedOidsResponse> responseObserver) {
        Objects.requireNonNull(responseObserver);
        if (Objects.requireNonNull(request).hasBroadcastTimeUtcMs() && request.getAliasIdToRealIdCount() > 0) {
            final Collection<OidMapping> oidMappings = request.getAliasIdToRealIdMap().entrySet().stream()
                .map(entry -> createOidMapping(entry.getKey(), entry.getValue(), request.getBroadcastTimeUtcMs()))
                .collect(Collectors.toSet());
            try {
                sqlOidMappingStore.registerOidMappings(oidMappings);
            } catch (Exception ex) {
                logger.error("Exception on registering oid mappings", ex);
                responseObserver.onError(ex);
            }
        } else {
            if (!request.hasBroadcastTimeUtcMs()) {
                responseObserver.onError(new RuntimeException("UploadAliasedOidsRequest has no broadcast time set"));
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
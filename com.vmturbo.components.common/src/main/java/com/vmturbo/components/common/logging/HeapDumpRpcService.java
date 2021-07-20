package com.vmturbo.components.common.logging;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLogEntry;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.common.protobuf.logging.HeapDumpServiceGrpc.HeapDumpServiceImplBase;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.DumpHeapRequest;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.DumpHeapResponse;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.EnableHeapDumpRequest;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.GetHeapDumpEnabledRequest;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.HeapDumpEnabledResponse;
import com.vmturbo.common.protobuf.memory.HeapDumper;

/**
 * Service to trigger heap dumps.
 */
public class HeapDumpRpcService extends HeapDumpServiceImplBase {
    private final HeapDumper heapDumper;

    private static final Logger logger = LogManager.getLogger();

    private final String parentComponentName;

    /**
     * When the user does not provide a filename to dump the heap to, the default to use.
     */
    public static final String DEFAULT_HEAP_DUMP_FILENAME = "/tmp/heap.dump";

    /**
     * Whether heap dumping is enabled. If false, all attempts to trigger a heap dump will be rejected
     * until heap dumping is enabled.
     */
    private boolean enableHeapDumping;

    /**
     * Create a new {@link HeapDumpRpcService}.
     *
     * @param heapDumper The object to use to dump the heap.
     * @param parentComponentName The name of the XL component with which this service is associated.
     * @param enableHeapDumping Whether the heap dump service is initially enabled. If false, any attempts to
     *                          trigger a heap dump will be rejected.
     */
    public HeapDumpRpcService(@Nonnull final HeapDumper heapDumper,
                              @Nonnull final String parentComponentName,
                              final boolean enableHeapDumping) {
        this.heapDumper = Objects.requireNonNull(heapDumper);
        this.parentComponentName = Objects.requireNonNull(parentComponentName);
        this.enableHeapDumping = enableHeapDumping;
    }

    @Override
    public void dumpHeap(DumpHeapRequest request,
                         StreamObserver<DumpHeapResponse> responseObserver) {

        sendAuditLogMessage(String.format("Attempt to trigger heap dump to file '%s'", request.getFilename()),
            parentComponentName, true);
        if (!enableHeapDumping) {
            sendAuditLogMessage("Heap dump failed because the service is disabled.",
                parentComponentName, false);
            responseObserver.onError(Status.FAILED_PRECONDITION
                .withDescription("Heap Dumping is disabled. You must enable before a dump will succeed.")
                .asException());
            return;
        }

        final String filename = request.getFilename().isEmpty()
            ? DEFAULT_HEAP_DUMP_FILENAME : request.getFilename();

        try {
            final String response = heapDumper.dumpHeap(filename);
            responseObserver.onNext(DumpHeapResponse.newBuilder()
                .setResponseMessage(response).build());
            sendAuditLogMessage("Heap dump succeeded", parentComponentName, true);
        } catch (Exception e) {
            logger.error("Failed to dump heap to " + filename + ": ", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
            sendAuditLogMessage("Heap dump failed", parentComponentName, false);
        }
        responseObserver.onCompleted();
    }

    @Override
    public void toggleDumpEnabled(EnableHeapDumpRequest request,
                                  StreamObserver<HeapDumpEnabledResponse> responseObserver) {
        sendAuditLogMessage(String.format("Setting heap dump enabled=%b", request.getEnabled()),
            parentComponentName, true);
        enableHeapDumping = request.getEnabled();

        responseObserver.onNext(HeapDumpEnabledResponse.newBuilder().setEnabled(enableHeapDumping).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getHeapDumpEnabled(GetHeapDumpEnabledRequest request,
                                   StreamObserver<HeapDumpEnabledResponse> responseObserver) {
        logger.info("getHeapDumpEnabled: enabled={}", enableHeapDumping);
        responseObserver.onNext(HeapDumpEnabledResponse.newBuilder().setEnabled(enableHeapDumping).build());
        responseObserver.onCompleted();
    }

    private static void sendAuditLogMessage(@Nonnull final String message,
                                            @Nonnull final String parentComponentName,
                                            final boolean result) {
        final AuditLogEntry triggerEntry = new AuditLogEntry.Builder(AuditAction.HEAP_DUMP,
            message, result)
            .targetName(parentComponentName)
            .build();
        AuditLogUtils.audit(triggerEntry);
    }

    /**
     * Factory for creating new {@link HeapDumpRpcService} instance.
     */
    public static class Factory {
        private final HeapDumper heapDumper;
        private final boolean removeHeapDumpService;
        private final boolean enableHeapDumping;

        /**
         * Create a new {@link HeapDumpRpcService.Factory}.
         *
         * @param heapDumper The name of the parent component.
         * @param removeHeapDumpService Whether to entirely remove the heap dumping service. If true, the heap dump
         *                              service will be unavailable in the component.
         * @param enableHeapDumping If the heap dump service is not removed, whether heap dumping is initially
         *                          enabled or disabled.
         */
        public Factory(@Nonnull final HeapDumper heapDumper,
                       final boolean removeHeapDumpService,
                       final boolean enableHeapDumping) {
            this.heapDumper = Objects.requireNonNull(heapDumper);
            this.removeHeapDumpService = removeHeapDumpService;
            this.enableHeapDumping = enableHeapDumping;
        }

        /**
         * Create a new HeapDumpRpcService instance.
         *
         * @param parentComponentName The name of the parent component that owns the service.
         * @return A new {@link HeapDumpRpcService}.
         */
        @Nullable
        public HeapDumpRpcService instance(@Nonnull final String parentComponentName) {
            if (removeHeapDumpService) {
                sendAuditLogMessage("Heap dumping service removed", parentComponentName, true);
                return null;
            } else {
                sendAuditLogMessage(String.format("Heap dumping service is being created with state enabled=%b",
                    enableHeapDumping), parentComponentName, true);
                return new HeapDumpRpcService(heapDumper, parentComponentName, enableHeapDumping);
            }
        }
    }
}

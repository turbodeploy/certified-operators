package com.vmturbo.components.common.logging;

import java.util.Objects;

import javax.annotation.Nonnull;

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
     * The name of the parent component for the service. Used when logging audit messages to be able
     * to tell what component is associated.
     */
    private String parentComponentName;

    /**
     * Create a new {@link HeapDumpRpcService}.
     *
     * @param heapDumper The object to use to dump the heap.
     */
    public HeapDumpRpcService(@Nonnull final HeapDumper heapDumper) {
        this.heapDumper = Objects.requireNonNull(heapDumper);
        // Initialize heap dump enabled to false. The #initialize call may adjust when the parent
        // component name is set.
        this.enableHeapDumping = false;
    }

    /**
     * Initialize the heap dump service. The service will reject all requests (because it will be disabled)
     * until initialized.
     *
     * @param enableHeapDumping Whether heap dumping should be initially enabled or disabled.
     * @param parentComponentName The name of the parent component.
     */
    public void initialize(final boolean enableHeapDumping,
                           @Nonnull final String parentComponentName) {
        this.enableHeapDumping = enableHeapDumping;
        this.parentComponentName = Objects.requireNonNull(parentComponentName);

        sendAuditLogMessage(String.format("Heap dump service initialized with state enabled=%b", enableHeapDumping),
            parentComponentName, true);
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

    /**
     * Send a heap-dump related audit log message.
     *
     * @param message The message to send.
     * @param parentComponentName The parent component of the heap dump service.
     * @param result The success or failure status of the action being audited.
     */
    public static void sendAuditLogMessage(@Nonnull final String message,
                                            @Nonnull final String parentComponentName,
                                            final boolean result) {
        final AuditLogEntry triggerEntry = new AuditLogEntry.Builder(AuditAction.HEAP_DUMP,
            message, result)
            .targetName(parentComponentName)
            .build();
        AuditLogUtils.audit(triggerEntry);
    }
}

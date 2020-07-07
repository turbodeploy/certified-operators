package com.vmturbo.components.common.logging;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.logging.MemoryMetrics.DumpHeapRequest;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.DumpHeapResponse;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.FindMemoryPathRequest;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.FindMemoryPathResponse;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.ListWalkableRootsRequest;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.ListWalkableRootsResponse;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.ListWalkableRootsResponse.RootObject;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.MemoryMetricsConfiguration;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.MemoryMetricsConfiguration.MemoryGraph;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.WalkAllRootObjectsRequest;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.WalkRootObjectResponse;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.WalkRootObjectsRequest;
import com.vmturbo.common.protobuf.logging.MemoryMetricsServiceGrpc.MemoryMetricsServiceImplBase;
import com.vmturbo.common.protobuf.memory.HeapDumper;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.MemoryPathVisitor;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.NamedObject;
import com.vmturbo.components.common.metrics.MemoryMetricsManager;
import com.vmturbo.components.common.metrics.MemoryMetricsManager.ManagedRoot;
import com.vmturbo.components.common.metrics.MemoryMetricsManager.WalkBuilder;
import com.vmturbo.components.common.utils.TimeUtil;

/**
 * Service that allows configuration of memory metrics collected and logged by a
 * component. Note that by default collection of these metrics should be disabled.
 */
public class MemoryMetricsRpcService extends MemoryMetricsServiceImplBase {
    private static final Logger logger = LogManager.getLogger();

    /**
     * When the user does not provide a filename to dump the heap to, the default to use.
     */
    public static final String DEFAULT_HEAP_DUMP_FILENAME = "/tmp/heap.dump";
    private final HeapDumper heapDumper;

    /**
     * Create new MemoryMetricsRpcService instance.
     *
     * @param heapDumper The object to use to dump the heap.
     */
    public MemoryMetricsRpcService(@Nonnull final HeapDumper heapDumper) {
        this.heapDumper = Objects.requireNonNull(heapDumper);
    }

    @Override
    public void walkRootObjects(WalkRootObjectsRequest request,
                                StreamObserver<WalkRootObjectResponse> responseObserver) {
        final Instant startTime = Clock.systemUTC().instant();
        final WalkRootObjectResponse.Builder builder = WalkRootObjectResponse.newBuilder();
        final Set<String> requestedRootNames = new HashSet<>(request.getRootSetNamesList());
        final Set<NamedObject> rootSet = getRootSet(requestedRootNames);

        final Set<Object> exclusionSet;
        if (request.getExcludeOtherRoots()) {
            exclusionSet = Collections.newSetFromMap(new IdentityHashMap<>());
            exclusionSet.addAll(MemoryMetricsManager.getManagedRootsCopy().keySet());
        } else {
            exclusionSet = Collections.emptySet();
        }

        try {
            final String results = performMemoryWalk(rootSet, request.getWalkConfiguration(),
                exclusionSet, request.getExclusionDepth());

            final List<String> walkedRoots = rootSet.stream()
                .map(root -> root.name)
                .collect(Collectors.toList());
            requestedRootNames.removeAll(walkedRoots);

            builder.addAllWalkedRootNames(walkedRoots);
            builder.addAllNotWalkedRootNames(requestedRootNames);
            processAndLogResults(results, builder, startTime, request.getResponseLineCount());
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error during walkRootObjects:", e);
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withCause(e)
                .withDescription(e.getMessage())
                .asException());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void walkAllRootObjects(WalkAllRootObjectsRequest request,
                                   StreamObserver<WalkRootObjectResponse> responseObserver) {
        final Instant startTime = Clock.systemUTC().instant();
        final WalkRootObjectResponse.Builder builder = WalkRootObjectResponse.newBuilder();
        final Map<Object, ManagedRoot> managedRoots = MemoryMetricsManager.getManagedRootsCopy();
        final List<NamedObject> roots = managedRoots.values().stream()
            .map(root -> new NamedObject(root.name, root.rootObj))
            .collect(Collectors.toList());

        try {
            final String results = performMemoryWalk(roots, request.getWalkConfiguration(),
                Collections.emptySet(), 0);

            processAndLogResults(results, builder, startTime, request.getResponseLineCount());
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error during walkAllRootObjects:", e);
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withCause(e)
                .withDescription(e.getMessage())
                .asException());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void listWalkableRoots(ListWalkableRootsRequest request,
                                  StreamObserver<ListWalkableRootsResponse> responseObserver) {
        final ListWalkableRootsResponse.Builder builder = ListWalkableRootsResponse.newBuilder();
        MemoryMetricsManager.getManagedRootsCopy().forEach((obj, root) ->
            builder.addWalkableRoots(RootObject.newBuilder()
                .setName(root.name)
                .setOwnerComponent(root.ownerComponentName)
                .setClassName(obj.getClass().getName())));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void findMemoryPath(FindMemoryPathRequest request,
                               StreamObserver<FindMemoryPathResponse> responseObserver) {
        final Instant startTime = Clock.systemUTC().instant();
        final FindMemoryPathResponse.Builder builder = FindMemoryPathResponse.newBuilder();
        final Set<Class<?>> resolvedClasses = new HashSet<>();
        final List<String> unresolvedClassNames = resolveClasses(request.getClassNamesList(), resolvedClasses);

        final Stream<ManagedRoot> rootStream;
        if (request.getRootSetNamesCount() > 0) {
            final Set<String> rootNames = new HashSet<>(request.getRootSetNamesList());
            rootStream = MemoryMetricsManager.getManagedRootsCopy().values().stream()
                .filter(root -> rootNames.contains(root.name));
        } else {
            rootStream = MemoryMetricsManager.getManagedRootsCopy().values().stream();
        }
        final List<NamedObject> roots = rootStream.map(root -> new NamedObject(root.name, root.rootObj))
            .collect(Collectors.toList());

        try {
            final MemoryPathVisitor paths = MemoryMetricsManager.newWalk()
                .maxDepth(request.getWalkDepth())
                .findPaths(request.getMaxInstances(), request.getMinInstanceDepth(),
                    roots, resolvedClasses);
            logger.info(paths.tabularResults());

            builder.addAllResolvedClasses(resolvedClasses.stream().map(Class::getName)
                .collect(Collectors.toList()));
            builder.addAllUnresolvedClasses(unresolvedClassNames);
            builder.addAllPaths(paths.foundPaths(request.getIncludeStringValues()));
            builder.setRequestDuration(TimeUtil.humanReadable(
                Duration.between(startTime, Clock.systemUTC().instant())));
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error during findMemoryPath:", e);
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withCause(e)
                .withDescription(e.getMessage())
                .asException());
            responseObserver.onCompleted();
        }
    }

    private List<String> resolveClasses(@Nonnull final List<String> classNamesList,
                                        @Nonnull final Set<Class<?>> resolvedClasses) {
        final List<String> unresolved = new ArrayList<>();
        for (final String klassName : classNamesList) {
            try {
                resolvedClasses.add(Class.forName(klassName));
            } catch (ClassNotFoundException e) {
                logger.info("Unable to resolve class {}", klassName);
            }
        }
        return unresolved;
    }

    @Override
    public void dumpHeap(DumpHeapRequest request,
                         StreamObserver<DumpHeapResponse> responseObserver) {
        final String filename = request.getFilename().isEmpty()
            ? DEFAULT_HEAP_DUMP_FILENAME : request.getFilename();

        try {
            final String response = heapDumper.dumpHeap(filename);
            responseObserver.onNext(DumpHeapResponse.newBuilder()
                .setResponseMessage(response).build());
        } catch (Exception e) {
            logger.error("Failed to dump heap to " + filename + ": ", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
        responseObserver.onCompleted();
    }

    /**
     * Perform a memory walk with the given configuration.
     *
     * @param roots The root objects to start the walk from.
     * @param walkConfiguration The configuration describing the type of walk to perform.
     * @param exclusionSet Objects to exclude from the walk.
     * @param exclusionDepth Depth at which to start applying exclusions to the walk.
     * @return A description of the walk results.
     */
    private String performMemoryWalk(@Nonnull final Collection<NamedObject> roots,
                                     @Nonnull final MemoryMetricsConfiguration walkConfiguration,
                                     @Nonnull final Set<Object> exclusionSet,
                                     final int exclusionDepth) {
        final WalkBuilder walkBuilder = MemoryMetricsManager.newWalk()
            .withExclusions(exclusionSet, exclusionDepth)
            .maxDepth(walkConfiguration.getWalkDepth());

        switch (walkConfiguration.getWalkTypeCase()) {
            case SIZE_AND_COUNT:
                return walkBuilder.walkSizesAndCounts(
                    roots.stream().map(r -> r.rootObj).collect(Collectors.toList()))
                    .toString();
            case CLASS_HISTOGRAM:
                return walkBuilder.walkClassHistogram(
                    roots.stream().map(r -> r.rootObj).collect(Collectors.toList()))
                    .toString();
            case MEMORY_GRAPH:
                final MemoryGraph mgConfig = walkConfiguration.getMemoryGraph();
                return walkBuilder.walkMemoryGraph(mgConfig.getLogDepth(), false, roots)
                    .tabularResults(mgConfig.getMinimumLogSizeBytes());
        }

        throw new IllegalArgumentException("WalkType " + walkConfiguration.getWalkTypeCase() + " not supported.");
    }

    /**
     * Process and log the results of a memory walk.
     *
     * @param walkResults A String description of the walk results.
     * @param builder A protobuf builder where the results should be set.
     * @param startTime The time at which the walk started.
     * @param resultLines The number of lines from the result to attach to the builder.
     */
    private void processAndLogResults(@Nonnull final String walkResults,
                                      @Nonnull final WalkRootObjectResponse.Builder builder,
                                      @Nonnull final Instant startTime,
                                      final int resultLines) {
        // Log the complete results
        logger.info(walkResults);

        int lines = (int)(walkResults.chars().filter(x -> x == '\n').count())
            + ((walkResults.endsWith("\n")) ? 0 : 1);

        // Set the results to be returned to the client.
        builder.setTotalResponseLines(lines);
        int ndx = StringUtils.ordinalIndexOf(walkResults, "\n", resultLines);
        final String results = ndx < 0 ? walkResults : walkResults.substring(0, ndx);
        for (String r : results.split("\n")) {
            builder.addWalkResults(r);
        }
        builder.setRequestDuration(TimeUtil.humanReadable(
            Duration.between(startTime, Clock.systemUTC().instant())));
    }

    /**
     * Get the root set of objects for a memory walk from the available roots.
     *
     * @param rootNames The names of the root set objects to walk.
     * @return The root set matching the names.
     */
    private Set<NamedObject> getRootSet(@Nonnull final Set<String> rootNames) {
        final Set<NamedObject> rootSet = Collections.newSetFromMap(new IdentityHashMap<>());
        MemoryMetricsManager.getManagedRootsCopy().forEach((obj, managedRoot) -> {
            if (rootNames.contains(managedRoot.name)) {
                rootSet.add(new NamedObject(managedRoot.name, managedRoot.rootObj));
            }
        });

        return rootSet;
    }
}

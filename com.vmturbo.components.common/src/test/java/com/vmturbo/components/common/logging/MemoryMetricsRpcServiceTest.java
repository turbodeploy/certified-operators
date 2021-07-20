package com.vmturbo.components.common.logging;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.logging.MemoryMetrics.FindMemoryPathRequest;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.FindMemoryPathResponse;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.ListWalkableRootsRequest;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.ListWalkableRootsResponse;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.ListWalkableRootsResponse.RootObject;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.MemoryMetricsConfiguration;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.MemoryMetricsConfiguration.ClassHistogram;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.MemoryMetricsConfiguration.SizeAndCount;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.WalkRootObjectResponse;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.WalkRootObjectsRequest;
import com.vmturbo.common.protobuf.logging.MemoryMetricsServiceGrpc;
import com.vmturbo.common.protobuf.logging.MemoryMetricsServiceGrpc.MemoryMetricsServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.metrics.MemoryMetricsManager;

/**
 * Tests for {@link MemoryMetricsRpcService}.
 */
public class MemoryMetricsRpcServiceTest {

    private final MemoryMetricsRpcService memoryMetricsRpcService = new MemoryMetricsRpcService();

    /**
     * grpcServer.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(memoryMetricsRpcService);

    private MemoryMetricsServiceBlockingStub memoryMetricsService;

    /**
     * TestObject.
     */
    private static class TestObject {
        private TestObject other;
        private String name;

        /**
         * Constructor for TestObject.
         *
         * @param name The name.
         */
        private TestObject(final String name) {
            other = null;
            this.name = name;
        }

        /**
         * Constructor for TestObject.
         *
         * @param other The other TestObject.
         * @param name The name.
         */
        private TestObject(TestObject other,
                          final String name) {
            this.other = other;
            this.name = name;
        }
    }

    // Foo --> Baz--> Quux
    //                /
    // Bar  ---------
    //
    // Foo and Bar are roots
    private static final TestObject quux = new TestObject("quux");
    private static final TestObject baz = new TestObject(quux, "baz");
    private static final TestObject bar = new TestObject(quux, "bar");
    private static final TestObject foo = new TestObject(baz, "foo");

    /**
     * setup.
     */
    @BeforeClass
    public static void setupClass() {
        // Set up the memory metrics manager for the test
        MemoryMetricsManager.clearManagedRoots();
        MemoryMetricsManager.addToManagedRootSet(foo.name, foo, "component");
        MemoryMetricsManager.addToManagedRootSet(bar.name, bar, "component");
    }

    /**
     * teardown.
     */
    @AfterClass
    public static void teardownClass() {
        // Tear down the memory metrics manager
        MemoryMetricsManager.clearManagedRoots();
    }

    /**
     * setup.
     */
    @Before
    public void setup() {
        memoryMetricsService = MemoryMetricsServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    /**
     * testListWalkableRoots.
     */
    @Test
    public void testListWalkableRoots() {
        final ListWalkableRootsResponse listWalkableRootsResponse =
            memoryMetricsService.listWalkableRoots(ListWalkableRootsRequest.getDefaultInstance());
        assertThat(listWalkableRootsResponse.getWalkableRootsList().stream()
                .map(RootObject::getName)
                .collect(Collectors.toList()),
            containsInAnyOrder(foo.name, bar.name));
    }

    /**
     * testWalkSingleRoot.
     */
    @Test
    public void testWalkSingleRoot() {
        final WalkRootObjectsRequest req = WalkRootObjectsRequest.newBuilder()
            .setWalkConfiguration(MemoryMetricsConfiguration.newBuilder().setSizeAndCount(
                SizeAndCount.getDefaultInstance()))
            .addRootSetNames("foo")
            .build();
        final WalkRootObjectResponse resp = memoryMetricsService.walkRootObjects(req);

        assertThat(resp.getWalkedRootNamesList(), contains("foo"));
        assertThat(resp.getNotWalkedRootNamesList(), is(empty()));
        assertThat(resp.getWalkResultsList().stream().collect(Collectors.joining("\n")), containsString("COUNT="));
        assertThat(resp.getWalkResultsList().stream().collect(Collectors.joining("\n")), containsString("SIZE="));
        assertTrue(resp.hasRequestDuration());
    }

    /**
     * testWalkRootNotPresent.
     */
    @Test
    public void testWalkRootNotPresent() {
        final WalkRootObjectsRequest req = WalkRootObjectsRequest.newBuilder()
            .setWalkConfiguration(MemoryMetricsConfiguration.newBuilder().setSizeAndCount(
                SizeAndCount.getDefaultInstance()))
            .addRootSetNames("asdf")
            .build();
        final WalkRootObjectResponse resp = memoryMetricsService.walkRootObjects(req);

        assertThat(resp.getWalkedRootNamesList(), is(empty()));
        assertThat(resp.getNotWalkedRootNamesList(), contains("asdf"));
        assertThat(resp.getWalkResultsList().stream().collect(Collectors.joining("\n")),
            containsString("COUNT=0, SIZE=0 Bytes"));
        assertTrue(resp.hasRequestDuration());
    }

    /**
     * testWalkWithSomePresent.
     */
    @Test
    public void testWalkWithSomePresent() {
        final WalkRootObjectsRequest req = WalkRootObjectsRequest.newBuilder()
            .setWalkConfiguration(MemoryMetricsConfiguration.newBuilder().setSizeAndCount(
                SizeAndCount.getDefaultInstance()))
            .addRootSetNames("foo")
            .addRootSetNames("asdf")
            .build();
        final WalkRootObjectResponse resp = memoryMetricsService.walkRootObjects(req);

        assertThat(resp.getWalkedRootNamesList(), contains("foo"));
        assertThat(resp.getNotWalkedRootNamesList(), contains("asdf"));
        assertThat(resp.getWalkResultsList().stream().collect(Collectors.joining("\n")), containsString("COUNT="));
        assertThat(resp.getWalkResultsList().stream().collect(Collectors.joining("\n")), containsString("SIZE="));
        assertTrue(resp.hasRequestDuration());
    }

    /**
     * testWalkHistogram.
     */
    @Test
    public void testWalkHistogram() {
        final WalkRootObjectsRequest req = WalkRootObjectsRequest.newBuilder()
            .setWalkConfiguration(MemoryMetricsConfiguration.newBuilder().setClassHistogram(
                ClassHistogram.getDefaultInstance()))
            .addRootSetNames("foo")
            .build();
        final WalkRootObjectResponse resp = memoryMetricsService.walkRootObjects(req);

        assertThat(resp.getWalkedRootNamesList(), contains("foo"));
        assertThat(resp.getWalkResultsList().stream().collect(Collectors.joining("\n")),
            containsString("TOTAL"));
        assertThat(resp.getWalkResultsList().stream().collect(Collectors.joining("\n")),
            containsString("MemoryMetricsRpcServiceTest$TestObject"));
        assertEquals(resp.getTotalResponseLines(), resp.getWalkResultsList().size());
        assertTrue(resp.hasRequestDuration());
    }

    /**
     * testFindPaths.
     */
    @Test
    public void testFindPaths() {
        final FindMemoryPathRequest req = FindMemoryPathRequest.newBuilder()
            .addClassNames(String.class.getName())
            .setMaxInstances(2)
            .setMinInstanceDepth(2)
            .addRootSetNames("foo")
            .build();
        final FindMemoryPathResponse resp = memoryMetricsService.findMemoryPath(req);

        assertEquals(2, resp.getPathsCount());
        assertEquals(String.class.getName(), resp.getPaths(0).getClassName());
        assertEquals("foo.other.name", resp.getPaths(0).getPath());
        assertEquals(String.class.getName(), resp.getPaths(1).getClassName());
        assertEquals("foo.other.other.name", resp.getPaths(1).getPath());
        assertTrue(resp.hasRequestDuration());
    }
}
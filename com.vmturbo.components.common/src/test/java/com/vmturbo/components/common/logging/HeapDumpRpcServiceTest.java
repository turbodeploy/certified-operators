package com.vmturbo.components.common.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.StatusRuntimeException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.logging.HeapDumpServiceGrpc;
import com.vmturbo.common.protobuf.logging.HeapDumpServiceGrpc.HeapDumpServiceBlockingStub;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.DumpHeapRequest;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.DumpHeapResponse;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.EnableHeapDumpRequest;
import com.vmturbo.common.protobuf.logging.MemoryMetrics.GetHeapDumpEnabledRequest;
import com.vmturbo.common.protobuf.memory.HeapDumper;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Tests for dumping the heap via RPC call.
 */
public class HeapDumpRpcServiceTest {

    private final HeapDumper heapDumper = Mockito.mock(HeapDumper.class);
    private final HeapDumpRpcService heapDumpRpcService = new HeapDumpRpcService(heapDumper);
    private final HeapDumpRpcService heapDumpRpcServiceDisabled = new HeapDumpRpcService(heapDumper);

    /**
     * grpcServer.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(heapDumpRpcService);

    /**
     * Disabled grpcServer.
     */
    @Rule
    public GrpcTestServer grpcServerDisabled = GrpcTestServer.newServer(heapDumpRpcServiceDisabled);

    /** Expected exceptions to test against. */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private HeapDumpServiceBlockingStub heapDumpService;

    /**
     * setup.
     */
    @Before
    public void setup() {
        heapDumpService = HeapDumpServiceGrpc.newBlockingStub(grpcServer.getChannel());
        heapDumpRpcService.initialize(true, getClass().getSimpleName());
        heapDumpRpcServiceDisabled.initialize(false, getClass().getSimpleName());
    }

    /**
     * testDumpHeapWithNoName.
     *
     * @throws Exception If heap dump throws exception.
     */
    @Test
    public void testDumpHeapWithNoName() throws Exception {
        when(heapDumper.dumpHeap(anyString())).thenReturn("foo");
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);

        final DumpHeapResponse response =
            heapDumpService.dumpHeap(DumpHeapRequest.getDefaultInstance());
        assertEquals("foo", response.getResponseMessage());
        verify(heapDumper).dumpHeap(argument.capture());
        assertEquals(HeapDumpRpcService.DEFAULT_HEAP_DUMP_FILENAME, argument.getValue());
    }

    /**
     * testDumpHeapWithName.
     *
     * @throws Exception If heap dump throws exception.
     */
    @Test
    public void testDumpHeapWithName() throws Exception {
        when(heapDumper.dumpHeap(anyString())).thenReturn("foo");
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);

        final DumpHeapResponse response =
            heapDumpService.dumpHeap(DumpHeapRequest.newBuilder()
                .setFilename("/foo/bar/heap.dump").build());
        assertEquals("foo", response.getResponseMessage());
        verify(heapDumper).dumpHeap(argument.capture());
        assertEquals("/foo/bar/heap.dump", argument.getValue());
    }

    /**
     * Test that heap dumping fails when disabled.
     *
     * @throws Exception If unexpected exception is thrown.
     */
    @Test
    public void testHeapDumpWhenDisabled() throws Exception {
        heapDumpService = HeapDumpServiceGrpc.newBlockingStub(grpcServerDisabled.getChannel());

        expectedException.expect(StatusRuntimeException.class);
        heapDumpService.dumpHeap(DumpHeapRequest.getDefaultInstance());
        verify(heapDumper, never()).dumpHeap(any());
    }

    /**
     * Test that enabling heap dump after it is initially disabled will permit dumping the heap.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testEnablingHeapDump() throws Exception {
        heapDumpService = HeapDumpServiceGrpc.newBlockingStub(grpcServerDisabled.getChannel());
        assertFalse(heapDumpService.getHeapDumpEnabled(GetHeapDumpEnabledRequest.getDefaultInstance()).getEnabled());
        heapDumpService.toggleDumpEnabled(EnableHeapDumpRequest.newBuilder().setEnabled(true).build());
        assertTrue(heapDumpService.getHeapDumpEnabled(GetHeapDumpEnabledRequest.getDefaultInstance()).getEnabled());

        when(heapDumper.dumpHeap(anyString())).thenReturn("foo");
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);

        final DumpHeapResponse response =
            heapDumpService.dumpHeap(DumpHeapRequest.getDefaultInstance());
        assertEquals("foo", response.getResponseMessage());
        verify(heapDumper).dumpHeap(argument.capture());
        assertEquals(HeapDumpRpcService.DEFAULT_HEAP_DUMP_FILENAME, argument.getValue());
    }
}

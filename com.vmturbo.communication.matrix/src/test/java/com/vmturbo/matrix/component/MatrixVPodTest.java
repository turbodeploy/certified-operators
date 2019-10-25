package com.vmturbo.matrix.component;

import org.junit.Assert;
import org.junit.Test;

/**
 * The VPoD tests.
 */
public class MatrixVPodTest {
    /**
     * Test vpod creation.
     */
    @Test
    public void testCreate() {
        OverlayVertex source = new OverlayVertex("10.10.10.2", 3000);
        OverlayVertex sink = new OverlayVertex("10.10.10.3", 3000);
        OverlayEdge edge = new OverlayEdge(1, 10, 7, 3, source, sink);
        MatrixVPod vpod = new MatrixVPod(edge, null);
        Assert.assertEquals(10, vpod.getPodFlow(), 0.001f);
    }

    /**
     * Test multiple vpods.
     */
    @Test
    public void testMulti() {
        OverlayVertex source = new OverlayVertex("10.10.10.2", 3000);
        OverlayVertex sink = new OverlayVertex("10.10.10.3", 3000);
        OverlayEdge edge = new OverlayEdge(1, 10, 7, 3, source, sink);
        MatrixVPod vpod = new MatrixVPod(edge, null);
        Assert.assertEquals(10, vpod.getPodFlow(), 0.0001D);

        OverlayVertex sink2 = new OverlayVertex("10.10.10.4", 3000);
        OverlayEdge edge2 = new OverlayEdge(16, 128, 64, 100, source, sink2);
        vpod.modify(edge2, null);
        Assert.assertEquals(138, vpod.getPodFlow(), 0.0001D);
    }

    /**
     * Test multiple vpods update.
     */
    @Test
    public void testMultiUpdateAll() {
        OverlayVertex source = new OverlayVertex("10.10.10.2", 3000);
        OverlayVertex sink = new OverlayVertex("10.10.10.3", 80);
        OverlayEdge edge = new OverlayEdge(1, 10, 5, 5, source, sink);
        MatrixVPod vpod = new MatrixVPod(edge, null);
        Assert.assertEquals(10, vpod.getPodFlow(), 0.0001D);

        OverlayVertex sink2 = new OverlayVertex("10.10.10.4", 8080);
        OverlayEdge edge2 = new OverlayEdge(15, 128, 10, 10, source, sink2);
        vpod.modify(edge2, null);
        Assert.assertEquals(138, vpod.getPodFlow(), 0.0001D);
        // The edge is the same.
        // The weight class, latency and flow all changed.
        OverlayEdge edge3 = new OverlayEdge(16, 129, 10, 11, source, sink2);
        vpod.modify(edge3, null);
        Assert.assertEquals(139, vpod.getPodFlow(), 0.0001D);
    }

    /**
     * Test multiple vpods update flow.
     */
    @Test
    public void testMultiUpdateFlow() {
        OverlayVertex source = new OverlayVertex("10.10.10.2", 0);
        OverlayVertex sink = new OverlayVertex("10.10.10.3", 80);
        OverlayEdge edge = new OverlayEdge(1, 10, 5, 5, source, sink);
        MatrixVPod vpod = new MatrixVPod(edge, null);
        Assert.assertEquals(10, vpod.getPodFlow(), 0.001f);

        OverlayVertex sink2 = new OverlayVertex("10.10.10.4", 8080);
        OverlayEdge edge2 = new OverlayEdge(16, 128, 10, 110, source, sink2);
        vpod.modify(edge2, null);
        Assert.assertEquals(138, vpod.getPodFlow(), 0.0001D);
        // The edge is the same.
        // The flow has changed.
        OverlayEdge edge3 = new OverlayEdge(16, 129, 64, 65, source, sink2);
        vpod.modify(edge3, null);
        Assert.assertEquals(139, vpod.getPodFlow(), 0.0001D);
        Assert.assertEquals(2, vpod.getEdges().size());
    }

    /**
     * Stress test.
     */
    @Test
    public void stressTest() {
        OverlayVertex source = new OverlayVertex("10.11.10.2", 0);
        OverlayVertex sink = new OverlayVertex("10.11.10.3", 80);
        OverlayEdge edge = new OverlayEdge(1, 10, 5, 5, source, sink);
        MatrixVPod vpod = new MatrixVPod(edge, null);
        for (int i = 0; i < 10000; i++) {
            String ip = "1.1." + ((i / 255) % 255) + "." + (i % 255);
            OverlayVertex sink1 = new OverlayVertex(ip, 80);
            OverlayEdge edge1 = new OverlayEdge(3, 11, 3, 8, source, sink1);
            vpod.modify(edge1, null);
        }
        Assert.assertEquals(10 + 110000, vpod.getPodFlow(), 0.0001D);
    }

    /**
     * Test vertices equality.
     */
    @Test
    public void testVertexEquals() {
        OverlayVertex source = new OverlayVertex("10.11.10.2", 8089);
        OverlayVertex source2 = new OverlayVertex("10.11.10.2", 0);
        OverlayVertex source3 = new OverlayVertex("10.11.10.2", 8080);
        OverlayVertex sink = new OverlayVertex("10.11.10.3", 80);
        // Different ports, but one is 0 (any port)
        Assert.assertNotEquals(source2, source3);
        Assert.assertNotEquals(source, source2);
        Assert.assertEquals(sink, sink);
        Assert.assertNotEquals(source, source3);
        Assert.assertNotEquals(source, sink);
        Assert.assertNotEquals(source, null);
        Assert.assertNotEquals(source, 1f);
    }

    /**
     * Test bad vertex.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testBadVertex() {
        new OverlayVertex("10.11.10.2", -1);
    }

    /**
     * Simple vertex test.
     */
    @Test
    public void testVertex() {
        OverlayVertex vertex = new OverlayVertex("10.11.10.2", 8080);
        Assert.assertEquals(OverlayVertex.normalizeIPAddress("10.11.10.2"),
                            vertex.getNormalizedIpAddress());
        Assert.assertEquals(8080, vertex.getPort());
    }

    /**
     * Test bad edge latency.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testBadEdgeLatency() {
        OverlayVertex source = new OverlayVertex("10.11.10.2", 0);
        OverlayVertex sink = new OverlayVertex("10.11.10.3", 80);
        new OverlayEdge(-1, 10, 5, 5, source, sink);
    }

    /**
     * Test bad edge flow.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testBadEdgeFlow() {
        OverlayVertex source = new OverlayVertex("10.11.10.2", 0);
        OverlayVertex sink = new OverlayVertex("10.11.10.3", 80);
        new OverlayEdge(1, -10, 0, 0, source, sink);
    }

    /**
     * Tests edges equality.
     */
    @Test
    public void testEdgeEquals() {
        OverlayVertex source = new OverlayVertex("10.11.10.2", 0);
        OverlayVertex sink1 = new OverlayVertex("10.11.10.3", 80);
        OverlayVertex sink2 = new OverlayVertex("10.11.10.4", 80);
        OverlayEdge edge1 = new OverlayEdge(1, 10, 7, 3, source, sink1);
        OverlayEdge edge2 = new OverlayEdge(1, 10, 6, 4, source, sink2);
        Assert.assertEquals(1, edge1.getLatency());
        Assert.assertEquals(edge1, edge1);
        Assert.assertEquals(edge2, edge2);
        Assert.assertEquals(edge1.getSource(), source);
        Assert.assertEquals(edge1.getSink(), sink1);
        Assert.assertNotEquals(edge1, edge2);
        Assert.assertNotEquals(edge1, null);
        Assert.assertNotEquals(edge1, source);
    }

    /**
     * Stress tests edges equality.
     */
    @Test
    public void testEdgeStressEquals() {
        OverlayVertex source = new OverlayVertex("10.11.10.2", 0);
        OverlayVertex sink = new OverlayVertex("10.11.10.3", 80);
        OverlayEdge edge1 = new OverlayEdge(1, 10, 0, 5, source, sink);
        OverlayEdge edge2 = new OverlayEdge(1, 10, 7, 8, source, sink);
        for (int i = 0; i < 10000; i++) {
            Assert.assertEquals(edge1, edge2);
        }

        for (int i = 0; i < 10000; i++) {
            Assert.assertEquals(edge1.hashCode(), edge2.hashCode());
        }
    }
}

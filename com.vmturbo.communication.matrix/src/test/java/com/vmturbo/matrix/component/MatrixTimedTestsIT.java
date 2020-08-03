package com.vmturbo.matrix.component;

import static com.vmturbo.matrix.component.CommunicationMatrixTest.constructEdges;

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.common.dto.CommonDTO.FlowDTO;
import com.vmturbo.platform.common.dto.CommonDTO.FlowDTO.Protocol;

/**
 * The timed integration tests.
 */
public class MatrixTimedTestsIT {
    /**
     * Set things up.
     */
    @Before
    public void setup() {
        System.setProperty("vmt.matrix.updatecycle", "0");
        IdentityGenerator.initPrefix(100L);
    }

    /**
     * Simple test with delay.
     *
     * @throws Exception If there's an error.
     */
    @Test
    public void testSimpleWithDelay() throws Exception {
        System.setProperty("vmt.matrix.updatecycle", "1000");
        MatrixInterface matrix = new CommunicationMatrix();
        matrix.update(CommunicationMatrixTest.constructEdges(1, 1, 8080, 1, 1, 10,
                                                             Protocol.TCP));
        Thread.sleep(2000L);
        Set<MatrixVPod> pods = CommunicationMatrixTest.getUniqueVPods((CommunicationMatrix)matrix);
        Assert.assertEquals(1, pods.size());
    }

    /**
     * Simple multiple segments.
     *
     * @throws Exception If there's an error.
     */
    @Test
    public void testMultipleSegments() throws Exception {
        System.setProperty("vmt.matrix.updatecycle", "1000");
        CommunicationMatrix matrix = new CommunicationMatrix();
        // First network segment.
        List<FlowDTO> edgesSegment1 = CommunicationMatrixTest.constructEdges(2, 1, 8080, 1, 1,
                                                                             10, Protocol.TCP);
        List<FlowDTO> edgesSegment2 = CommunicationMatrixTest.constructEdges(2, 100, 8080, 100, 1,
                                                                             10, Protocol.TCP);
        matrix.update(edgesSegment1);
        // Add next network segment. We expect the first to be intact.
        matrix.update(edgesSegment2);
        Assert.assertEquals(4, matrix.overlayNetwork_.state_.edges_.size());
        for (OverlayEdge edge : matrix.overlayNetwork_.state_.edges_) {
            Assert.assertEquals(10, edge.getFlow(), 0.001);
        }
        Thread.sleep(100L);
        // Add next network segment again. We expect the first to be intact, since not enough
        // time passed.
        matrix.update(edgesSegment2);
        Assert.assertEquals(4, matrix.overlayNetwork_.state_.edges_.size());
        for (OverlayEdge edge : matrix.overlayNetwork_.state_.edges_) {
            Assert.assertEquals(10, edge.getFlow(), 0.001);
        }
        Thread.sleep(1000L);
        // Add next network segment again. We expect the first to be diminished, since we passed
        // through the initial timeout for edges.
        matrix.update(edgesSegment2);
        Assert.assertEquals(4, matrix.overlayNetwork_.state_.edges_.size());
        for (OverlayEdge edge : matrix.overlayNetwork_.state_.edges_) {
            final String srcAddr = edge.getSink().getNormalizedIpAddress();
            if (srcAddr.substring(srcAddr.lastIndexOf('.') + 1).length() == 1) {
                Assert.assertEquals(10 * .3, edge.getFlow(), 0.001);
            } else {
                Assert.assertEquals(10, edge.getFlow(), 0.001);
            }
        }
        // Update again, should remove all previous edges the next update.
        Thread.sleep(1200L);
        matrix.update(edgesSegment2);
        // Remove previous edges
        Thread.sleep(1200L);
        // Add next network segment again. We expect the first to be diminished, since we passed
        // through the initial timeout for edges.
        matrix.update(edgesSegment2);
        Assert.assertEquals(2, matrix.overlayNetwork_.state_.edges_.size());
        for (OverlayEdge edge : matrix.overlayNetwork_.state_.edges_) {
            Assert.assertEquals(10, edge.getFlow(), 0.001);
        }
    }

    /**
     * Test mtarix cleaning.
     *
     * @throws InterruptedException If there's an error.
     */
    @Test
    public void testCleaningMatrix() throws InterruptedException {
        System.setProperty("vmt.matrix.cleanupcycle", "500");
        int count = 10;
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(count, -1, 8080, 1, 1, 1,
                                     Protocol.TCP));
        Assert.assertNotNull(matrix.hoover_.thread_);
        matrix.hoover_.thread_.join(2000L);
        Thread.sleep(1000L);
        Assert.assertTrue(matrix.overlayNetwork_.isEmpty());
    }

    /**
     * Test matrix cleaning with first sleep.
     */
    @Test
    public void testCleaningMatrixInterruptFirstSleep() {
        System.setProperty("vmt.matrix.cleanupcycle", "500");
        int count = 10;
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(count, -1, 8080, 1, 1, 1000000,
                                     Protocol.TCP));
        Assert.assertNotNull(matrix.hoover_.thread_);
        Object o = matrix.hoover_.thread_;
        matrix.hoover_.start();
        Assert.assertTrue(o == matrix.hoover_.thread_);
        matrix.hoover_.stop();
        Assert.assertNull(matrix.hoover_.thread_);
    }

    /**
     * Test matrix cleaning with second sleep.
     *
     * @exception InterruptedException In case of a test being stopped.
     */
    @Test
    public void testCleaningMatrixInterruptSecondSleep() throws InterruptedException {
        System.setProperty("vmt.matrix.cleanupcycle", "100");
        int count = 10;
        CommunicationMatrix matrix = new CommunicationMatrix();
        matrix.update(constructEdges(count, -1, 8080, 1, 1, 1000000,
                                     Protocol.TCP));
        Assert.assertNotNull(matrix.hoover_.thread_);
        Thread.sleep(1000L);
        matrix.hoover_.stop();
        Assert.assertNull(matrix.hoover_.thread_);
        // We first wait 2 * 100ms before starting to fade edges.
        // So, we have .3 ^ 8 as a reduction factor.
        double reductionFactor = .3 * .3 * .3 * .3 * .3 * .3 * .3 * .3;
        Assert.assertEquals(1000000. * reductionFactor,
                            matrix.overlayNetwork_.state_.edges_.iterator().next().getFlow(),
                            0.00001);
    }

    /**
     * It is here because Verizon DNS will resolve any unknown address to a default one.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNormalizeBadAddress() {
        OverlayVertex.normalizeIPAddress("no:abcd:such.place:123:FEC.co.mo:::on");
    }
}

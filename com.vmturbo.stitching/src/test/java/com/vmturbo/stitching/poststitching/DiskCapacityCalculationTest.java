package com.vmturbo.stitching.poststitching;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.IopsItemNames;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;

public class DiskCapacityCalculationTest {

    private static final double HYBRID_FACTOR = 10;
    private static final double FLASH_FACTOR = 1000;
    private static final EntitySettingsCollection mockEntitySettingsCollection =
            Mockito.mock(EntitySettingsCollection.class);
    private static final TopologyEntity mockTopologyEntity = Mockito.mock(TopologyEntity.class);

    /**
     * The below calculator is initialized for test purposes to a capacity of 1 for each disk type.
     * Therefore the base capacity for an entity is simply the number of disks.
     * This base capacity is multiplied by the above factors if the flags are set;
     * therefore for test purposes the above flags have values chosen to be huge and distinguishable.
     */
    private final DiskCapacityCalculator calculator = new DiskCapacityCalculator(1, 1, 1, 1, 1, 1,
        HYBRID_FACTOR, FLASH_FACTOR);

    @Test
    public void testNonMatchingString() {
        assertEquals(0, calculator.calculateCapacity("", mockEntitySettingsCollection, mockTopologyEntity), 1e-5);
        assertEquals(0, calculator.calculateCapacity("badstringbadstring", mockEntitySettingsCollection, mockTopologyEntity), 1e-5);
    }

    @Test
    public void testNumIopsSupported() {
        final String propString = makePropertyStringSegment(IopsItemNames.NUM_IOPS_SUPPORTED.name(), 225000);
        assertEquals(225000, calculator.calculateCapacity(propString, mockEntitySettingsCollection, mockTopologyEntity), 1e-5);
    }

    @Test
    public void testSingleDiskType() {
        final String propString = makePropertyStringSegment(IopsItemNames.NUM_7200_DISKS.name(), 4);
        assertEquals(4, calculator.calculateCapacity(propString, mockEntitySettingsCollection, mockTopologyEntity), 1e-5);
    }

    @Test
    public void testDuplicateDiskType() {
        final String propString = makePropertyString(IopsItemNames.NUM_7200_DISKS.name(), 4,
            IopsItemNames.NUM_7200_DISKS.name(), 10);
        assertEquals(14, calculator.calculateCapacity(propString, mockEntitySettingsCollection, mockTopologyEntity), 1e-5);
    }

    @Test
    public void testZeroDiskType() {
        final String propString = makePropertyString(IopsItemNames.NUM_7200_DISKS.name(), 4,
            IopsItemNames.NUM_15K_DISKS.name(), 0);
        assertEquals(4, calculator.calculateCapacity(propString, mockEntitySettingsCollection, mockTopologyEntity), 1e-5);
    }

    @Test
    public void testMultipleDiskTypes() {
        final String propString = makePropertyString(IopsItemNames.NUM_7200_DISKS.name(), 4,
            IopsItemNames.NUM_10K_DISKS.name(), 10);
        assertEquals(14, calculator.calculateCapacity(propString, mockEntitySettingsCollection, mockTopologyEntity), 1e-5);
    }

    @Test
    public void testFlags() {
        final String hybridFalse = "hybrid: false\n" + makePropertyStringSegment(IopsItemNames.NUM_10K_DISKS.name(), 1);
        final String hybridTrue = "hybrid: true\n" + makePropertyStringSegment(IopsItemNames.NUM_10K_DISKS.name(), 1);
        final String bothFalse = makePropertyString(false, false, IopsItemNames.NUM_10K_DISKS.name(), 1);
        final String hybridFalseFlashTrue = makePropertyString(false, true, IopsItemNames.NUM_10K_DISKS.name(), 1);
        final String bothTrueHybridWins = makePropertyString(true, true, IopsItemNames.NUM_10K_DISKS.name(), 1);

        assertEquals(1, calculator.calculateCapacity(bothFalse, mockEntitySettingsCollection, mockTopologyEntity), 1e-5);
        assertEquals(1, calculator.calculateCapacity(hybridFalse, mockEntitySettingsCollection, mockTopologyEntity), 1e-5);
        assertEquals(HYBRID_FACTOR, calculator.calculateCapacity(bothTrueHybridWins, mockEntitySettingsCollection, mockTopologyEntity), 1e-5);
        assertEquals(HYBRID_FACTOR, calculator.calculateCapacity(hybridTrue, mockEntitySettingsCollection, mockTopologyEntity), 1e-5);
        assertEquals(FLASH_FACTOR, calculator.calculateCapacity(hybridFalseFlashTrue, mockEntitySettingsCollection, mockTopologyEntity), 1e-5);
    }

    private String makePropertyString(final String diskTypeKey1, final int numDisks1,
                                      final String diskTypeKey2, final int numDisks2) {
        return makePropertyStringSegment(diskTypeKey1, numDisks1) +
            makePropertyStringSegment(diskTypeKey2, numDisks2);
    }

    private String makePropertyString(final boolean hybridFlag, final boolean flashFlag,
                                     final String diskTypeKey, final int numDisks) {
        return "hybrid: " + hybridFlag + "\nflashAvailable: " + flashFlag + "\n" +
            makePropertyStringSegment(diskTypeKey, numDisks);
    }

    private String makePropertyStringSegment(final String diskTypeKey, final int numDisks) {
        return "iopsItems {\n  iopsItemName: \"" + diskTypeKey + "\"\n  iopsItemValue: " + numDisks + "\n}\n";
    }
}

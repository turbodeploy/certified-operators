package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Collection;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.DiskTypeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.IopsItemData;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.IopsItemNames;

/**
 * Populate the common data in {@link TypeSpecificInfo} unique to an related entity - i.e.
 * {@link DiskTypeInfo}. We currently have the sub-class types disk array, logical pool and
 * storage controller.
 **/
public abstract class DiskTypeInfoMapper extends TypeSpecificInfoMapper {

    private static final Logger logger = LogManager.getLogger();

    protected void setupIopsComputeData(@Nonnull final String displayName,
        @Nonnull final Collection<IopsItemData> iopsItems,
        @Nonnull final DiskTypeInfo.Builder dtInfo) {
        iopsItems.forEach(iopsItemData -> {
            if (!iopsItemData.hasIopsItemName() || !iopsItemData.hasIopsItemValue()) {
                logger.warn("Missing iops item name or value in disk type aware entity " +
                    "{}. Iops items list : {}", displayName, iopsItems);
                return;
            }
            try {
                setupDisksCount(dtInfo, iopsItemData);
            } catch (IllegalArgumentException e) {
                logger.debug("Unknown field {} in DiskArrayInfo DTO.",
                    iopsItemData.getIopsItemName());
            }
        });
    }

    /**
     * Setup all of the iops items data to {@link DiskTypeInfo}.
     *
     * @param dtInfo The aspect {@link DiskTypeInfo} we need to fill up.
     * @param iopsItemData The iops item data from DTO.
     */
    private void setupDisksCount(@Nonnull final DiskTypeInfo.Builder dtInfo,
        @Nonnull final IopsItemData iopsItemData) {
        final IopsItemNames itemName = IopsItemNames.valueOf(iopsItemData.getIopsItemName());
        final long itemValue = iopsItemData.getIopsItemValue();
        switch (itemName) {
            case NUM_SSD:
                dtInfo.setNumSsd(itemValue);
                break;
            case NUM_7200_DISKS:
                dtInfo.setNum7200Disks(itemValue);
                break;
            case NUM_10K_DISKS:
                dtInfo.setNum10KDisks(itemValue);
                break;
            case NUM_15K_DISKS:
                dtInfo.setNum15KDisks(itemValue);
                break;
            case NUM_VSERIES_DISKS:
                dtInfo.setNumVSeriesDisks(itemValue);
        }
    }
}

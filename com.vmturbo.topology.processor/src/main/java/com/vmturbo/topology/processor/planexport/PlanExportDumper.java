package com.vmturbo.topology.processor.planexport;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.time.Instant;

import javax.annotation.Nonnull;

import net.jpountz.lz4.LZ4FrameOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.PlanExport.PlanExportDTO;

/**
 * Class to dump details of a plan export to a file for debugging purposes.
 */
public class PlanExportDumper {
    private final Logger logger = LogManager.getLogger();

    private static final String LZ4ZIP_FILE_SUFFIX = ".lz4";

    /**
     * Create a dumper.
     */
    public PlanExportDumper() {
    }

    /**
     * Dump details of a plan export to a file for debugging purposes.
     *
     * @param destination the destination to dump
     * @param destinationOid the oid of the destination
     * @param planData the plan data to dump
     * @param planId the oid of the plan
     * @param dumpDirectory the directory in which to create the dump file.
     */
    public void dumpPlanExportDetails(@Nonnull File dumpDirectory,
                                      @Nonnull NonMarketEntityDTO destination,
                                      long destinationOid,
                                      @Nonnull PlanExportDTO planData,
                                      long planId) {
        logger.trace("Starting plan export details dump");

        try {
            FileUtils.forceMkdir(dumpDirectory);
        } catch (IOException ex) {
            logger.error("Failed to create plan export details dump directory", ex);
            return;
        }

        final String dumpFileName = String.format("dest-%d-plan-%d-%s.txt%s",
            destinationOid, planId, Instant.now().toString(), LZ4ZIP_FILE_SUFFIX);

        File dumpFile = new File(dumpDirectory, dumpFileName);

        try (OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(dumpFile))) {
            os.write("Destination:\n\n".getBytes(Charset.defaultCharset()));
            os.write(destination.toString().getBytes(Charset.defaultCharset()));
            os.write("\n\nPlan Data:\n\n".getBytes(Charset.defaultCharset()));
            os.write(planData.toString().getBytes(Charset.defaultCharset()));
            logger.trace("Successfully saved plan export details");
        } catch (IOException ex) {
            logger.error("Could not save " + dumpFile.getAbsolutePath(), ex);
        }
    }
}

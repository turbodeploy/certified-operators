package com.vmturbo.topology.processor.discoverydumper;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.protobuf.InvalidProtocolBufferException;

import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.sdk.server.common.DiscoveryDumper;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Class to create and manage binary dump files constructed from target discovery responses.
 *
 * <p>Dump files are saved in the hosting component's file system.</p>
 *
 * <p>Only one binary file per target is allowed. These files will be used by topology processor
 * after a restart, to load back information about previous discovery responses.
 * </p>
 */
public class BinaryDiscoveryDumper implements DiscoveryDumper {
    /**
     * Directory where to store discovery dumps.
     */
    private final File dumpDirectory;
    /**
     * Logger to use.
     */
    private final Logger logger = LogManager.getLogger(getClass());


    /**
     * Constructs the instance of dump file repository, pointing at specific dump files directory.
     *
     * @param dumpDirectory directory to use to store dumps
     * @throws IOException if an error occurs in creating the directory
     */
    public BinaryDiscoveryDumper(@Nonnull final File dumpDirectory) throws IOException {
        this.dumpDirectory = Objects.requireNonNull(dumpDirectory);
        FileUtils.forceMkdir(dumpDirectory);
    }


    /**
     * Dumps a discovery response into a gzipped text file.
     *
     * @param tgtId target id, used to distinguish dumps across the targets.
     * @param discoveryType type of the discovery.
     * @param discovery discovery result itself.
     * @param accountDefs information about hidden fields that should not appear in the dump.
     */
    @Override
    public synchronized void dumpDiscovery(
          @Nonnull final String tgtId,
          @Nonnull final DiscoveryType discoveryType,
          @Nonnull final DiscoveryResponse discovery,
          @Nonnull final List<AccountDefEntry> accountDefs) {

        if (discoveryType == DiscoveryType.INCREMENTAL) {
            return;
        }
        sweepOldFileForTarget(tgtId);

        dump(
            tgtId,
            discovery,
            discoveryType);

    }

    private void dump(
          String tgtId,
          DiscoveryResponse discoveryResponse,
          DiscoveryType discoveryType) {
        final DiscoveryDumpFilename ddf =
              new DiscoveryDumpFilename(tgtId, new Date(), discoveryType);
        final File dtoFile = ddf.getFile(dumpDirectory, false, true);

        logger.debug("Absolute path for text dump file: {}", dtoFile::getAbsolutePath);

        try (OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(dtoFile))) {
            os.write(discoveryResponse.toByteArray());
            logger.trace("Successfully saved text discovery response");
        } catch (IOException e) {
            logger.error("Could not save " + dtoFile.getAbsolutePath(), e);
        }
    }

    private void sweepOldFileForTarget( @Nonnull String tgtId) {
        logger.trace("Sweeping old dump for target {}", tgtId);
        final String[] allDiscoveryDumpFiles = dumpDirectory.list();
        if (allDiscoveryDumpFiles == null) {
            logger.error("Cannot get the list of discovery dump files");
            return;
        }
        for (String filename : allDiscoveryDumpFiles) {
            // parse filename
            final DiscoveryDumpFilename ddf = DiscoveryDumpFilename.parse(filename);
            if (ddf == null) {
                // not a valid dump file name
                continue;
            }
            if (ddf.getSanitizedTargetName().equals(tgtId)) {
                removeFile(ddf);
            }
        }
    }

    /**
     * Restore discovery responses based on the targets currently present in the target store.
     * @param targetStore target store contianing the targets
     * @return Map with discovery responses for each target id
     */
    public Map<Long, DiscoveryResponse> restoreDiscoveryResponses(@Nonnull TargetStore targetStore) {
        final String[] allDiscoveryDumpFiles = dumpDirectory.list();
        Map<Long, DiscoveryResponse> discoveryResponsesByTargetId = new HashMap<>();
        if (allDiscoveryDumpFiles == null) {
            logger.error("Cannot get the list of discovery dump files");
            return discoveryResponsesByTargetId;
        }
        for (String filename : allDiscoveryDumpFiles) {
            // parse filename
            final DiscoveryDumpFilename ddf = DiscoveryDumpFilename.parse(filename);
            if (ddf == null) {
                // not a valid dump file name
                continue;
            }

            // add to corresponding caching map
            final Long sanitizedTargetId = Long.valueOf(ddf.getSanitizedTargetName());
            // If the target is no longer in the target store, delete the file
            if (!targetStore.getTarget(sanitizedTargetId).isPresent()) {
                removeFile(ddf);
            } else {
                final File binaryFile = ddf.getFile(dumpDirectory, false, true);
                final DiscoveryResponse response;
                try {
                    response = DiscoveryResponse.parseFrom(readCompressedFile(binaryFile));
                    discoveryResponsesByTargetId.put(sanitizedTargetId, response);
                } catch (InvalidProtocolBufferException e) {
                    logger.error("Could not parse discovery response from file {}", binaryFile);
                } catch (IOException e) {
                    logger.error("Could not read discovery response from file {}", binaryFile);
                }
            }
        }
        return discoveryResponsesByTargetId;

    }

    private byte[] readCompressedFile(File file) throws IOException {
        LZ4FrameInputStream gis = new LZ4FrameInputStream(new FileInputStream(file));
        ByteArrayOutputStream fos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len;
        while ((len = gis.read(buffer)) != -1) {
            fos.write(buffer, 0, len);
        }
        fos.close();
        gis.close();
        return fos.toByteArray();
    }

    private void removeFile(@Nonnull DiscoveryDumpFilename ddf) {
        final File fileText = ddf.getFile(dumpDirectory, false, true);

        // delete the files
        if (!fileText.delete()) {
            logger.warn("Could not remove file {}", fileText::getAbsolutePath);
        } else {
            logger.debug("Removed file {}", fileText::getAbsolutePath);
        }

    }
}

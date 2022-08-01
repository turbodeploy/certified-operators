package com.vmturbo.voltron.extensions.tp;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.sdk.server.common.DiscoveryDumper;
import com.vmturbo.topology.processor.discoverydumper.DiscoveryDumpFilename;
import com.vmturbo.topology.processor.discoverydumper.DiscoveryDumperImpl;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * A discovery dumper for the topology processor cache that uses uncompressed human-readable
 * discovery response files (instead of compressed binary ones) so that they are immediately
 * accessible for developers.
 */
public class CacheOnlyDiscoveryDumper implements DiscoveryDumper {

    /**
     * Logger to use.
     */
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Directory where to store discovery dumps.
     */
    private final File dumpDirectory;

    /**
     * Constructs discovery dumper, pointing at specific dump files directory.
     *
     * @param dumpDirectory directory to use to store dumps
     */
    @Nonnull
    public CacheOnlyDiscoveryDumper(@Nonnull final File dumpDirectory) {
        this.dumpDirectory = Objects.requireNonNull(dumpDirectory);
        try {
            FileUtils.forceMkdir(dumpDirectory);
        } catch (IOException e) {
            logger.warn("Failed to create dump directory", e);
        }
    }

    /**
     * Returns the directory used to store discovery responses.
     *
     * @return The directory where discovery dumps are stored.
     */
    public File getDumpDirectory() {
        return dumpDirectory;
    }

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
        dump(tgtId,
                DiscoveryDumperImpl.removeHiddenInfoFromDiscovery(discovery, accountDefs),
                discoveryType);

    }

    private void dump(
            String tgtId,
            DiscoveryResponse discoveryResponse,
            DiscoveryType discoveryType) {
        final DiscoveryDumpFilename ddf =
                new DiscoveryDumpFilename(tgtId, new Date(), discoveryType);
        final File dtoFile = ddf.getFile(dumpDirectory, true, false);
        logger.debug("Dumping discovery response to file: {}", dtoFile::getAbsolutePath);
        try (OutputStream os = new FileOutputStream(dtoFile);
             BufferedOutputStream bos = new BufferedOutputStream(os)) {
            bos.write(discoveryResponse.toString().getBytes(Charset.defaultCharset()));
            logger.info("Successfully saved text discovery response: {}",
                    dtoFile.getName());
        } catch (IOException e) {
            logger.error("Could not save text discovery response: "
                    + dtoFile.getAbsolutePath(), e);
        }
    }

    private void sweepOldFileForTarget( @Nonnull String tgtId) {
        logger.info("Removing Discovery dump of target : {}", tgtId);
        final String[] allDiscoveryDumpFiles = dumpDirectory.list();
        if (allDiscoveryDumpFiles == null) {
            logger.error("Cannot get the list of discovery dump files");
            return;
        }
        for (String filename : allDiscoveryDumpFiles) {
            final DiscoveryDumpFilename ddf = DiscoveryDumpFilename.parse(filename);
            if (ddf == null) {
                // not a valid dump file name
                continue;
            }
            if (ddf.getSanitizedTargetName().equals(tgtId)) {
                logger.info("Removing discovery dump file: {}", filename);
                removeFile(ddf);
            }
        }
    }

    /**
     * Restores discovery responses from the topology processor cache.
     *
     * @param targetStore The target store.
     * @return a map of discovery response objects by target ID.
     */
    public Map<Long, DiscoveryResponse> restoreDiscoveryResponses(@Nonnull TargetStore targetStore) {
        Map<Long, DiscoveryResponse> discoveryResponsesByTargetId = new HashMap<>();
        final String[] allDiscoveryDumpFiles = dumpDirectory.list();
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
            Long sanitizedTargetId = null;
            // Expecting name like:
            //   707867536121655_Azure_Service_Principal_EA___Development-2022.07.08.15.47.36.376-FULL.txt"
            String[] parts = ddf.getSanitizedTargetName().split("_");
            if (parts.length > 0) {
                sanitizedTargetId = Longs.tryParse(parts[0]);
            }
            if (sanitizedTargetId == null) {
                continue;
            }
            // add to corresponding caching map
            // If the target is no longer in the target store, delete the file
            if (!targetStore.getTarget(sanitizedTargetId).isPresent()) {
                removeFile(ddf);
            } else {
                final DiscoveryResponse dr = getDiscoveryResponse(ddf);
                if (dr != null) {
                    discoveryResponsesByTargetId.put(sanitizedTargetId, dr);
                }
            }
        }
        return discoveryResponsesByTargetId;
    }

    /**
     * Gets a DiscoveryResponse given a DiscoveryDumpFilename.
     * @param ddf The DiscoveryDumpFilename
     * @return a DiscoveryResponse or null if an error occurs parsing.
     */
    public DiscoveryResponse getDiscoveryResponse(DiscoveryDumpFilename ddf) {
        DiscoveryResponse dr = null;
        final File drFile = ddf.getFile(dumpDirectory, true, false);
        try {
            byte[] bytes = Files.readAllBytes(drFile.toPath());
            dr = TextFormat.parse(new String(bytes, Charset.defaultCharset()),
                    DiscoveryResponse.class);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Could not parse discovery response from file {}", drFile);
        } catch (IOException e) {
            logger.error("Could not read discovery response from file {}", drFile);
        }
        return dr;
    }

    private void removeFile(@Nonnull DiscoveryDumpFilename ddf) {
        final File fileText = ddf.getFile(dumpDirectory, true, false);
        if (!fileText.delete()) {
            logger.warn("Could not remove file {}", fileText::getAbsolutePath);
        } else {
            logger.debug("Removed file {}", fileText::getAbsolutePath);
        }
    }
}

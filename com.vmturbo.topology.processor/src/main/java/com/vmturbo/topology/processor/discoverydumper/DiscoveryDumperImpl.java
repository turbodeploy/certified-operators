package com.vmturbo.topology.processor.discoverydumper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import net.jpountz.lz4.LZ4FrameOutputStream;

import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.DerivedTargetSpecificationDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.sdk.server.common.DiscoveryDumper;

/**
 * Class to create and manage dump files constructed from target discovery responses.
 *
 * <p>Dump files are saved in the hosting component's file system, and all available dumps are
 * included in diags zip files.</p>
 *
 * <p>Multiple dumps for a given target may be retained. Retentions are based on a discoveryDumpsToHold
 * config property obtained from a TargetDumpingSettings implementation class. When the conifigured
 * number of dumps already exist for a given target, a new dump will cause the oldest dumps for that
 * target to be removed until the count matches the setting.</p>
 *
 * <p>This class was largely copied from the class of the same name in Classic, however we add
 * support for gzip-compressing dump files.</p>
 */
public class DiscoveryDumperImpl implements DiscoveryDumper {
    /**
     * Directory where to store discovery dumps.
     */
    private final File dumpDirectory;
    /**
     * Logger to use.
     */
    private final Logger logger = LogManager.getLogger(getClass());
    /**
     * Target dump settings provider.
     */
    private final TargetDumpingSettings dumpSettings;
    /**
     * Caching: the first map maps sanitized target names to chronologically sorted sets of all
     * the dump files associated with them.
     * The second map maps maps sanitized target names to the number of full discoveries that
     * are associated with them.
     */
    private final Map<String, NavigableSet<DiscoveryDumpFilename>> dumpFileNames = new HashMap<>();
    private final Map<String, Integer> numOfFullDiscoveries = new HashMap<>();

    /**
     * Constructs the instance of dump file repository, pointing at specific dump files directory
     * and using the specified settings.
     *
     * @param dumpDirectory directory to use to store dumps
     * @param dumpSettings dumping settings provider
     */
    public DiscoveryDumperImpl(@Nonnull final File dumpDirectory,
            @Nonnull final TargetDumpingSettings dumpSettings) throws IOException {
        this.dumpDirectory = Objects.requireNonNull(dumpDirectory);
        this.dumpSettings = Objects.requireNonNull(dumpSettings);
        FileUtils.forceMkdir(dumpDirectory);
        initializeCaching();
    }

    @Nonnull
    private NavigableSet<DiscoveryDumpFilename> getDumpNamesForTarget(
          String target) {
        return dumpFileNames.computeIfAbsent(target, k -> new TreeSet<>());
    }

    private int getNumOfFullDiscoveries(String target) {
        return numOfFullDiscoveries.computeIfAbsent(target, k -> 0);
    }

    private void initializeCaching() {
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

            // add to corresponding caching map
            final String sanitizedTargetName = ddf.getSanitizedTargetName();
            final boolean ddfWasNotAlreadyThere =
                getDumpNamesForTarget(sanitizedTargetName).add(ddf);

            // count full discovery dumps (note that there are two files per full discovery dump
            // but they should count as a single full discovery)
            if (ddf.getDiscoveryType() == DiscoveryType.FULL && ddfWasNotAlreadyThere) {
                numOfFullDiscoveries.put(
                    sanitizedTargetName, getNumOfFullDiscoveries(sanitizedTargetName) + 1);
            }
        }
    }

    /**
     * Dumps a discovery response into a gzipped text file
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
        final String sanitizedTargetName = DiscoveryDumpFilename.sanitize(tgtId);

        // if dumping is off, then remove all dump files that correspond to this target
        final int dumpsToHold = dumpSettings.getDumpsToHold(tgtId);
        if (dumpsToHold <= 0) {
            logger.debug(
                "Discovery dumping for target {} is turned off. Skipped dumping",
                tgtId);

            // remove all existing discovery dump files for this target
            final Iterator<DiscoveryDumpFilename> iterator =
                getDumpNamesForTarget(sanitizedTargetName).iterator();
            while (iterator.hasNext()) {
                removeFile(iterator, iterator.next(), sanitizedTargetName);
            }

            return;
        }

        // if this is an empty incremental discovery, then omit dumping
        if (discoveryType == DiscoveryType.INCREMENTAL && discovery.getAllFields().isEmpty()) {
            return;
        }

        // perform the dump
        dump(
            sanitizedTargetName,
            removeHiddenInfoFromDiscovery(discovery, accountDefs),
            discoveryType);

        // if this is a full discovery, we may need to delete old dump files
        if (discoveryType == DiscoveryType.FULL) {
            sweepOldFiles(dumpsToHold, sanitizedTargetName);
        }
    }

    private void dump(
          String sanitizedTargetName,
          DiscoveryResponse discoveryResponse,
          DiscoveryType discoveryType) {
        final DiscoveryDumpFilename ddf =
              new DiscoveryDumpFilename(sanitizedTargetName, new Date(), discoveryType);
        final File txtFile = ddf.getFile(dumpDirectory, true, true);

        logger.debug("Absolute path for text dump file: {}", txtFile::getAbsolutePath);

        try (final OutputStream os = new LZ4FrameOutputStream(new FileOutputStream(txtFile))) {
            os.write(discoveryResponse.toString().getBytes(Charset.defaultCharset()));
            logger.trace("Successfully saved text discovery response");
        } catch (IOException e) {
            logger.error("Could not save " + txtFile.getAbsolutePath(), e);
        }

        getDumpNamesForTarget(sanitizedTargetName).add(ddf);
        if (ddf.getDiscoveryType() == DiscoveryType.FULL) {
            numOfFullDiscoveries.put(
                sanitizedTargetName, getNumOfFullDiscoveries(sanitizedTargetName) + 1);
        }
    }

    private void sweepOldFiles(
          int numOfFullDiscoveriesToHold, @Nonnull String sanitizedTargetName) {
        logger.trace("Sweeping old dumps for target {}", sanitizedTargetName);

        // remove old discoveries until number of full discoveries is
        // less than or equal to permitted
        final Iterator<DiscoveryDumpFilename> iterator =
            getDumpNamesForTarget(sanitizedTargetName).iterator();
        while (iterator.hasNext() &&
               getNumOfFullDiscoveries(sanitizedTargetName) > numOfFullDiscoveriesToHold) {
            removeFile(iterator, iterator.next(), sanitizedTargetName);
        }

        // remove old non-full discoveries until oldest permitted full discovery
        while (iterator.hasNext()) {
            final DiscoveryDumpFilename ddf = iterator.next();
            if (ddf.getDiscoveryType() == DiscoveryType.FULL) {
                break;
            }
            removeFile(iterator, ddf, sanitizedTargetName);
        }
    }

    private void removeFile(
          @Nonnull Iterator<DiscoveryDumpFilename> iterator,
          @Nonnull DiscoveryDumpFilename ddf,
          @Nonnull String sanitizedTargetName) {
        final File fileText = ddf.getFile(dumpDirectory, true, true);

        // delete the files
        if (!fileText.delete()) {
            logger.warn("Could not remove file {}", fileText::getAbsolutePath);
        } else {
            logger.debug("Removed file {}", fileText::getAbsolutePath);
        }

        // adjust the full discovery dump count, if the deletion was successful
        if (!fileText.exists()) {
            iterator.remove();
            if (ddf.getDiscoveryType() == DiscoveryType.FULL) {
                numOfFullDiscoveries.put(
                      sanitizedTargetName,
                      getNumOfFullDiscoveries(sanitizedTargetName) - 1);
            }
        }
    }

    @Nonnull
    private static DiscoveryResponse removeHiddenInfoFromDiscovery(
          @Nonnull DiscoveryResponse discovery,
          @Nonnull List<AccountDefEntry> accountDefs) {
        // Make a copy of the DiscoveryResponse which we will remove password account fields from
        //  before we the dump response
        Set<String> secretAccountFields = accountDefs.stream()
              .filter(acctDef -> acctDef.getCustomDefinition().getIsSecret())
              .map(acctDef -> acctDef.getCustomDefinition().getName())
              .collect(Collectors.toSet());
        DiscoveryResponse.Builder filteredResponse = DiscoveryResponse.newBuilder(discovery);
        List<DerivedTargetSpecificationDTO> cleanedTargets = Lists.newArrayList();
        for (DerivedTargetSpecificationDTO nextDerivedTarget :
              filteredResponse.getDerivedTargetList()) {
            DerivedTargetSpecificationDTO.Builder targetBuilder =
                  DerivedTargetSpecificationDTO.newBuilder(nextDerivedTarget);
            List<AccountValue> scrubbedAccountValues = nextDerivedTarget
                  .getAccountValueList()
                  .stream()
                  .filter(acct -> !secretAccountFields.contains(acct.getKey()))
                  .collect(Collectors.toList());
            targetBuilder.clearAccountValue();
            targetBuilder.addAllAccountValue(scrubbedAccountValues);
            cleanedTargets.add(targetBuilder.build());
        }
        filteredResponse.clearDerivedTarget();
        filteredResponse.addAllDerivedTarget(cleanedTargets);
        return filteredResponse.build();
    }
}

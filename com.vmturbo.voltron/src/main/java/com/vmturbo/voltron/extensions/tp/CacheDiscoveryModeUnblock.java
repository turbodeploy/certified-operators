package com.vmturbo.voltron.extensions.tp;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.primitives.Longs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.Discovery.DerivedTargetSpecificationDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.discoverydumper.DiscoveryDumpFilename;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.DerivedTargetParser;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;
import com.vmturbo.topology.processor.topology.pipeline.blocking.PipelineUnblock;

/**
 * This is a PipelineUnblock implementation that:
 * 1. Precreates targets if a precache target directory is found.
 * 2. Loads all discovery responses from the topology processor cache
 * 3. Unblocks the {@link TopologyPipelineExecutorService}
 */
public class CacheDiscoveryModeUnblock implements PipelineUnblock {
    private static final Logger logger = LogManager.getLogger();
    private static final String ACCOUNT_NAME = "name";
    private static final String ACCOUNT_KEY = "key";
    private static final String TARGETS_YAML = "targets.yaml";
    private final TopologyPipelineExecutorService pipelineExecutorService;
    private final ProbeStore probeStore;
    private final TargetStore targetStore;
    private final DerivedTargetParser derivedTargetParser;
    private final IOperationManager operationManager;
    private final IdentityProvider identityProvider;
    private CacheOnlyDiscoveryDumper discoveryDumper;

    CacheDiscoveryModeUnblock(@Nonnull final TopologyPipelineExecutorService pipelineExecutorService,
            @Nonnull final TargetStore targetStore,
            @Nonnull final DerivedTargetParser derivedTargetParser,
            @Nonnull final ProbeStore probeStore,
            @Nonnull final IOperationManager operationManager,
            @Nonnull final IdentityProvider identityProvider,
            CacheOnlyDiscoveryDumper discoveryDumper) {
        this.pipelineExecutorService = pipelineExecutorService;
        this.targetStore = targetStore;
        this.derivedTargetParser = Objects.requireNonNull(derivedTargetParser);
        this.probeStore = probeStore;
        this.operationManager = operationManager;
        this.identityProvider = identityProvider;
        this.discoveryDumper = discoveryDumper;
    }

    @Override
    public void run() {
        try {
            try {
                identityProvider.waitForInitializedStore();
                preCreateTargets();
                // restore the discovery dumps in the order they were first added as targets
                Map<Long, DiscoveryResponse> cachedResponses = discoveryDumper.restoreDiscoveryResponses(targetStore);
                logger.info("Restoring discovery dumps from cache, count={}", cachedResponses.size());
                cachedResponses.entrySet().stream()
                        .sorted(Comparator.comparingLong(
                                entry -> IdentityGenerator.toMilliTime(entry.getKey())))
                        .forEach(entry -> {
                            final long targetId = entry.getKey();
                            Optional<Target> target = targetStore.getTarget(targetId);
                            if (target.isPresent()) {
                                long probeId = target.get().getProbeId();
                                Discovery operation = new Discovery(probeId, targetId,
                                        identityProvider);

                                logger.info("Loaded cached discovery response for target={}", target.get().getDisplayName());
                                try {
                                    operationManager.notifyLoadedDiscovery(operation,
                                        entry.getValue()).get(20, TimeUnit.MINUTES);
                                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                    logger.error(
                                            "Error in notifying the discovery result for target "
                                                    + "id:{} and probe id: {}", targetId,
                                            probeId, e);
                                }
                            }
                        });
            } catch (RuntimeException e) {
                logger.error("Failed to restore discovery responses from disk.", e);
            } catch (InterruptedException e) {
                logger.error("Failed to restore discovery responses from disk due to "
                        + "time out in waiting for the identity store to be initialized", e);
            }
        } catch (RuntimeException e) {
            logger.error("Unexpected exception. Unblocking broadcasts early.", e);
        } finally {
            // Unblock the pipeline when done, even if we exit the loop due to some kind of exception.
            pipelineExecutorService.unblockBroadcasts();
        }
    }

    /**
     * Create targets found in precache dirs inside the topology processor dump dir.
     */
    private void preCreateTargets() {
        File[] targetPrecacheDirs = discoveryDumper.getDumpDirectory().listFiles(
                        f -> f.isDirectory() && f.getName().startsWith("target"));
        for (File targetPrecacheDir : targetPrecacheDirs) {
            preCreateTargets(targetPrecacheDir);
        }
    }

    private void preCreateTargets(File targetPrecacheDir) {
        File targetsFile = new File(targetPrecacheDir, TARGETS_YAML);
        if (targetsFile == null || !targetsFile.exists()) {
            return;
        }
        try {
            logger.info("Processing target precache dir: {}", targetPrecacheDir.getName());
            ObjectMapper om = new ObjectMapper(new YAMLFactory());
            TargetSet targetSet = om.readValue(targetsFile, TargetSet.class);
            if (!targetSet.isEnabled()) {
                logger.info("Skipping disabled target set for precache dir {}",
                        targetPrecacheDir);
                return;
            }
            for (TargetInfo targetInfo : targetSet.getTargets()) {
                try {
                    if (!targetInfo.isEnabled()) {
                        logger.info("Skipping disabled precache target: {}", targetInfo.getName());
                        continue;
                    }
                    TargetSpec tSpec = getTargetSpec(targetInfo);
                    String targetName = getTargetName(tSpec);
                    Optional<ProbeInfo> maybeProbe = probeStore.getProbe(tSpec.getProbeId());
                    if (!maybeProbe.isPresent()) {
                        logger.error("Probe not found for precached target={}, type={}, probeId={}",
                                targetName, targetInfo.getType(), tSpec.getProbeId());
                        continue;
                    }
                    ProbeInfo probeInfo = maybeProbe.get();
                    List<Target> allTargets = targetStore.getAll();
                    Optional<Target> parent = getTarget(allTargets, probeInfo.getProbeType(), targetName);
                    Target pTarget = parent.isPresent()
                            ? parent.get()
                            : targetStore.createTarget(tSpec);
                    DiscoveryResponse dr = getCachedDiscoveryResponse(targetInfo, pTarget.getId(),
                            targetPrecacheDir, probeInfo.getProbeType(), targetName);
                    if (dr == null) {
                        logger.warn("Skipping precache target, no precached DiscoveryResponse found, target={}, type={}",
                                targetName, probeInfo.getProbeType());
                        return;
                    }
                    preCreateDerivedTargets(targetInfo, targetPrecacheDir, pTarget, dr);
                } catch (Exception e) {
                    logger.error("Error processing precache target " + targetInfo.getName()
                            + ", precacheDir=" + targetPrecacheDir, e);
                }
            }
        } catch (Exception e) {
            logger.error("Error creating targets for precache dir "
                    + targetPrecacheDir.getName(), e);
        }
    }

    private DiscoveryResponse getCachedDiscoveryResponse(TargetInfo targetInfo,
            Long targetId,
            File targetPrecacheDir,
            String probeType,
            String targetName) {
        String sanitizedName = DiscoveryDumpFilename.sanitize(probeType + "_" + targetName);
        File preCacheDrFile = getCachedDiscoveryResponseFile(targetPrecacheDir, sanitizedName, null);
        if (preCacheDrFile == null) {
            return null;
        }
        // We have a precached DR for this target, check if one already exists in the normal cache dir
        // for the given targetId
        DiscoveryDumpFilename ddf = null;
        File cacheDrFile = getCachedDiscoveryResponseFile(discoveryDumper.getDumpDirectory(),
                sanitizedName, targetId);
        if (cacheDrFile != null) {
            ddf = DiscoveryDumpFilename.parse(cacheDrFile.getName());
            logger.info("Cached discovery response already exists, file={}, target={}/{}, type={}",
                    cacheDrFile.getName(), targetId, targetName, probeType);
        } else {
            DiscoveryDumpFilename cachedDdf = DiscoveryDumpFilename.parse(preCacheDrFile.getName());
            try {
                Path original = preCacheDrFile.toPath();
                // We have to copy the cached DR into the dump dir but first need to update its
                // stale targetID with the correct one
                String newCacheFileName = preCacheDrFile.getName();
                String sanitizedWithStaleTgId = cachedDdf.getSanitizedTargetName();
                // If cached discovery starts with a targetId, correct it
                String[] parts = splitSanitizedName(sanitizedWithStaleTgId);
                Long staleTargetId = Longs.tryParse(parts[0]);
                newCacheFileName = staleTargetId != null
                        ? newCacheFileName.replace(staleTargetId.toString(), targetId.toString())
                        : targetId + "_" + newCacheFileName;
                File dumpDirCopy = new File(discoveryDumper.getDumpDirectory(), newCacheFileName);
                logger.info("Copying discovery response to cache dir, file={}, target {}, type={}",
                        preCacheDrFile.getName(), targetName, probeType);
                Files.copy(original, dumpDirCopy.toPath(), StandardCopyOption.REPLACE_EXISTING);
                // The ddf must be the one in the dump dir to be parsed by binaryDiscoveryDumper
                ddf = DiscoveryDumpFilename.parse(newCacheFileName);
            } catch (IOException e) {
                logger.error("Failed to copy discovery response, file={}, target {}, type={}, error={}",
                        preCacheDrFile.getName(), targetName, probeType, e.getMessage());
            }
        }
        if (ddf != null) {
            final DiscoveryResponse dr = discoveryDumper.getDiscoveryResponse(ddf);
            return fixDerivedTargets(targetInfo, dr);
        }
        return null;
    }

    private static String[] splitSanitizedName(String sanitizedName) {
        return sanitizedName.split("_");
    }

    /**
     * Given a sanitized name such as "Azure_Subscription_Pay_As_You_Go", find an existing cached
     * discovery response in the given cache dir.
     *
     * @param cacheDir The cache dir to look in.
     * @param sanitizedName The sanitized name to look for.
     * @return A discovery response file that matches the sanitized name.
     */
    private File getCachedDiscoveryResponseFile(File cacheDir, String sanitizedName, Long targetId) {
        File[] drFiles = cacheDir.listFiles(f -> !f.isDirectory() && f.getName().endsWith(".txt"));
        for (File drFile : drFiles) {
            // sanitized name should have no target ID and might be:
            // "Azure_Subscription_Pay_As_You_Go"
            // An existing cached file might be:
            // 707838999440991_Azure_Subscription_Pay_As_You_Go___Product_Management
            // 1. If ID is passed, compare it with found target ID
            // 2. If ID matches or is null, compare the remaining value, use equals not contains
            // 3.
            final DiscoveryDumpFilename ddf = DiscoveryDumpFilename.parse(drFile.getName());
            if (ddf != null) {
                String otherSanitizedName = ddf.getSanitizedTargetName();
                // If cached discovery starts with a targetId, remove it
                String[] parts = splitSanitizedName(otherSanitizedName);
                Long otherTargetId = Longs.tryParse(parts[0]);
                if (otherTargetId != null) {
                    otherSanitizedName = otherSanitizedName.replace(otherTargetId + "_", "");
                }
                if (targetId != null) {
                    if (targetId.equals(otherTargetId) && sanitizedName.equals(otherSanitizedName)) {
                        return drFile;
                    }
                } else {
                    if (otherSanitizedName.equals(sanitizedName)) {
                        return drFile;
                    }
                }
            }
        }
        return null;
    }

    private String getTargetName(TargetSpec tSpec) {
        List<AccountValue> accountVals = tSpec.getAccountValueList().stream()
                .filter(av -> av.getKey().equals(ACCOUNT_NAME))
                .collect(Collectors.toList());
        if (accountVals.size() == 1) {
            return accountVals.get(0).getStringValue();
        }
        return null;
    }

    private String getTargetName(DerivedTargetSpecificationDTO derivedTargetSpec) {
        List<com.vmturbo.platform.common.dto.Discovery.AccountValue> accountVals =
                derivedTargetSpec.getAccountValueList().stream()
                        .filter(av -> av.getKey().equals(ACCOUNT_NAME))
                        .collect(Collectors.toList());
        if (accountVals.size() == 1) {
            return accountVals.get(0).getStringValue();
        }
        return null;
    }

    /**
     * A function which creates derived targets recursively.
     *
     * @param targetInfo The original target information
     * @param targetPrecacheDir The target precache dir being processed
     * @param parentTarget The parent target being processed
     * @param parentDiscoveryResp the parent's cached discovery response
     */
    private void preCreateDerivedTargets(TargetInfo targetInfo,
            File targetPrecacheDir,
            Target parentTarget,
            DiscoveryResponse parentDiscoveryResp) {
        List<DerivedTargetSpecificationDTO> derivedTargetSpecs = parentDiscoveryResp.getDerivedTargetList();
        if (derivedTargetSpecs == null || derivedTargetSpecs.size() == 0) {
            logger.debug("No derived targets found for target {}, type={}",
                    parentTarget.getDisplayName(), parentTarget.getProbeInfo().getProbeType());
            return;
        }

        logger.info("Processing derived targets of target {}, type={}, count={}",
                parentTarget.getDisplayName(), parentTarget.getProbeInfo().getProbeType(),
                derivedTargetSpecs.size());
        List<DerivedTargetSpecificationDTO> neededDerivedTargets = new ArrayList<>();
        for (DerivedTargetSpecificationDTO derivedTarget : derivedTargetSpecs) {
            List<Target> allTargets = targetStore.getAll();
            Optional<Target> t = getTarget(allTargets, derivedTarget.getProbeType(), getTargetName(derivedTarget));
            if (!t.isPresent()) {
                neededDerivedTargets.add(derivedTarget);
            }
        }
        if (neededDerivedTargets.size() > 0) {
            derivedTargetParser.instantiateDerivedTargets(parentTarget.getId(), neededDerivedTargets);
        }
        Set<Long> derivedTargetIds = targetStore.getDerivedTargetIds(parentTarget.getId());
        for (Long derivedTargetId : derivedTargetIds) {
            Target derivedTarget = targetStore.getTarget(derivedTargetId).get();
            DiscoveryResponse derivedDr = getCachedDiscoveryResponse(
                    targetInfo,
                    derivedTargetId,
                    targetPrecacheDir,
                    derivedTarget.getProbeInfo().getProbeType(),
                    derivedTarget.getDisplayName());
            if (derivedDr != null) {
                preCreateDerivedTargets(targetInfo, targetPrecacheDir,
                        derivedTarget, derivedDr);
            } else {
                logger.info("No cached discovery response found for derived target {}, type={}",
                        derivedTarget.getDisplayName(), derivedTarget.getProbeInfo().getProbeType());
            }
        }
    }

    private DiscoveryResponse fixDerivedTargets(TargetInfo targetInfo, DiscoveryResponse dr) {
        List<DerivedTargetSpecificationDTO> dts = dr.getDerivedTargetList();
        if (dts == null || dts.size() == 0) {
            return dr;
        }
        DiscoveryResponse.Builder builder = dr.toBuilder();
        builder.clearDerivedTarget();
        for (DerivedTargetSpecificationDTO dt : dts) {
            DerivedTargetSpecificationDTO.Builder dtBuilder = dt.toBuilder();
            // In cache only mode all targets are visible and can be rediscovered
            dtBuilder.setHidden(false);
            // If this account has a key/secret prop, provide the key if missing,
            // otherwise we will see an error when creating the derived target that it is required.
            String keyVal = targetInfo.getProperty(ACCOUNT_KEY);
            if (keyVal == null) {
                continue;
            }
            if (!dt.getAccountValueList()
                    .stream()
                    .filter(av -> av.getKey().equals(ACCOUNT_KEY))
                    .findFirst()
                    .isPresent()) {
                dtBuilder.addAccountValue(com.vmturbo.platform.common.dto.Discovery.AccountValue.newBuilder()
                        .setKey(ACCOUNT_KEY)
                        .setStringValue(keyVal)
                        .build());
            }
            builder.addDerivedTarget(dtBuilder);
        }
        return builder.build();
    }

    private Optional<Target> getTarget(List<Target> allTargets, String probeType,
            String targetName) {
        return allTargets.stream()
                .filter(t -> t.getProbeInfo().getProbeType().equalsIgnoreCase(probeType)
                        && t.getDisplayName().equals(targetName))
                .findFirst();
    }

    /**
     * Returns a target spec that can be used to create a new target from the given target info.
     *
     * @param targetInfo The target info.
     * @return A TargetSpec that can be used to create the target.
     */
    private TargetSpec getTargetSpec(TargetInfo targetInfo) {
        String probeType = targetInfo.getType();
        Optional<Long> probeId = probeStore.getProbeIdForType(probeType);
        if (!probeId.isPresent()) {
            logger.info("Failed load precached discovery responses, no probe found for probe type: "
                    + probeType);
            return null;
        }
        TargetSpec.Builder tSpecBuilder = TargetSpec.newBuilder();
        tSpecBuilder.setProbeId(probeId.get())
                .addAccountValue(AccountValue.newBuilder()
                        .setKey(ACCOUNT_NAME)
                        .setStringValue(targetInfo.getName())
                        .build());
        for (Entry<String, String> entrySet : targetInfo.getProperties().entrySet()) {
            tSpecBuilder.addAccountValue(AccountValue.newBuilder()
                    .setKey(entrySet.getKey())
                    .setStringValue(entrySet.getValue())
                    .build());
        }
        return tSpecBuilder.build();
    }

    /**
     * Represents a set of targets.
     */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    private static class TargetSet {
        private boolean enabled = true;
        private List<TargetInfo> targets = new ArrayList<>();

        List<TargetInfo> getTargets() {
            return targets;
        }

        boolean isEnabled() {
            return enabled;
        }
    }

    /**
     * Represents the setup information for a particular target.
     */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    private static class TargetInfo {
        private String name;
        private String type;
        private boolean enabled;
        private Map<String, String> props = new HashMap<>();

        String getName() {
            return name;
        }

        String getType() {
            return type;
        }

        boolean isEnabled() {
            return enabled;
        }

        Map<String, String> getProperties() {
            return props;
        }

        String getProperty(String name) {
            return props.get(name);
        }
    }
}

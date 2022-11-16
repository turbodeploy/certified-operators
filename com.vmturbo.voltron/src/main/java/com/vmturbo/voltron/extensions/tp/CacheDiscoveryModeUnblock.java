package com.vmturbo.voltron.extensions.tp;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.primitives.Longs;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.common.dto.CommonDTO.ConnectedEntity;
import com.vmturbo.platform.common.dto.CommonDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.MembersList;
import com.vmturbo.platform.common.dto.Discovery.DerivedTargetSpecificationDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
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
    private static final String ACCOUNT_USERNAME = "username";
    private static final String ACCOUNT_KEY = "key";
    private static final String TARGETS_YAML = "targets.yaml";
    private static final String DUPE_APPENDAGE = "_dupe_";
    private final TopologyPipelineExecutorService pipelineExecutorService;
    private final ProbeStore probeStore;
    private final TargetStore targetStore;
    private final DerivedTargetParser derivedTargetParser;
    private final IOperationManager operationManager;
    private final IdentityProvider identityProvider;
    private CacheOnlyDiscoveryDumper discoveryDumper;
    private boolean clearAllTargets;

    CacheDiscoveryModeUnblock(@Nonnull final TopologyPipelineExecutorService pipelineExecutorService,
            @Nonnull final TargetStore targetStore,
            @Nonnull final DerivedTargetParser derivedTargetParser,
            @Nonnull final ProbeStore probeStore,
            @Nonnull final IOperationManager operationManager,
            @Nonnull final IdentityProvider identityProvider,
            CacheOnlyDiscoveryDumper discoveryDumper,
            boolean clearAllTargets) {
        this.pipelineExecutorService = pipelineExecutorService;
        this.targetStore = targetStore;
        this.derivedTargetParser = Objects.requireNonNull(derivedTargetParser);
        this.probeStore = probeStore;
        this.operationManager = operationManager;
        this.identityProvider = identityProvider;
        this.discoveryDumper = discoveryDumper;
        this.clearAllTargets = clearAllTargets;
    }

    @Override
    public void run() {
        try {
            try {
                identityProvider.waitForInitializedStore();
                if (clearAllTargets) {
                    clearAllTargets();
                }
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

                                logger.info("Loaded cached discovery response for target={}, type={}",
                                        target.get().getDisplayName(), target.get().getProbeInfo().getProbeType());
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
                    Optional<Target> existingTarget = getTarget(allTargets, probeInfo.getProbeType(), targetName);
                    Target target = existingTarget.isPresent()
                            ? existingTarget.get()
                            : targetStore.createTarget(tSpec);
                    DiscoveryResponse dr = getCachedDiscoveryResponse(targetInfo, target.getId(),
                            targetPrecacheDir, probeInfo.getProbeType(), targetName);
                    if (dr == null) {
                        logger.warn("Skipping precache target, no precached DiscoveryResponse found, target={}, type={}",
                                targetName, probeInfo.getProbeType());
                        return;
                    }
                    List<DerivedTargetInfo> dtInfos = preCreateDerivedTargets(targetInfo, targetPrecacheDir, target, dr);
                    if (targetInfo.getCopies() > 0) {
                        duplicateTarget(targetInfo, dr, dtInfos);
                    }
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

    private void clearAllTargets() {
        try {
            logger.info("Clearing all targets");
            targetStore.removeAllTargets();
        } catch (Exception e) {
            logger.error("Failed to remove all targets", e);
        }
    }

    private void duplicateTarget(TargetInfo tInfo, DiscoveryResponse dr,
            List<DerivedTargetInfo> dtInfos) {
        for (int i = 0; i < tInfo.getCopies(); i++) {
            try {
                int dupeCount = i + 1;
                String dupeAppendage = DUPE_APPENDAGE + dupeCount;
                TargetInfo dupeTargetInfo = duplicateTargetInfo(tInfo, dupeAppendage);
                TargetSpec tSpec = getTargetSpec(dupeTargetInfo);
                Optional<Target> existingTarget = getExistingTarget(tSpec);
                Target dupeTarget;
                if (existingTarget.isPresent()) {
                    dupeTarget = existingTarget.get();
                } else {
                    // use createOrUpdate in case the appendage naming scheme changes, otherwise
                    // I'm seeing dupe target exceptions after tweaking it
                    dupeTarget = targetStore.createOrUpdateExistingTarget(tSpec, true);
                }
                String sanitizedName = DiscoveryDumpFilename.sanitize(dupeTarget.getProbeInfo().getProbeType() + "_" + dupeTarget.getDisplayName());
                File cacheDrFile = getCachedDiscoveryResponseFile(discoveryDumper.getDumpDirectory(),
                        sanitizedName, dupeTarget.getId());
                DiscoveryResponse dupeDr;
                if (cacheDrFile == null) {
                    DupeDiscoveryResponseContext ctx = new DupeDiscoveryResponseContext(tInfo, null,
                            null, dupeTarget, dr, dupeAppendage, -1);
                    dupeDr = saveDupeDiscoveryResponse(ctx);
                } else {
                    DiscoveryDumpFilename ddf = DiscoveryDumpFilename.parse(cacheDrFile.getName());
                    dupeDr = discoveryDumper.getDiscoveryResponse(ddf);
                }
                duplicateDerivedTargets(tInfo, dupeAppendage, dupeTarget, dr, dupeDr, dtInfos, dupeCount);
            } catch (Throwable t) {
                logger.error("Failed to duplicate target: " + tInfo.getName(), t);
            }
        }
    }

    /**
     * Encapsulation of Data needed to create Dupe DiscoveryResponses.
     */
    private class DupeDiscoveryResponseContext {
        private final TargetInfo targetInfo;
        private final DiscoveryResponse parentDr;
        private final DiscoveryResponse dupeParentDr;
        private final Target dupeTarget;
        private final DiscoveryResponse dr;
        private final String dupeAppendage;
        private final int dupeIndex;

        DupeDiscoveryResponseContext(TargetInfo targetInfo, DiscoveryResponse parentDr,
                DiscoveryResponse dupeParentDr, Target dupeTarget, DiscoveryResponse dr,
                String dupeAppendage, int dupeIndex) {
            this.targetInfo = targetInfo;
            this.parentDr = parentDr;
            this.dupeParentDr = dupeParentDr;
            this.dupeTarget = dupeTarget;
            this.dr = dr;
            this.dupeAppendage = dupeAppendage;
            this.dupeIndex = dupeIndex;
        }

        public TargetInfo getTargetInfo() {
            return targetInfo;
        }

        public DiscoveryResponse getParentDr() {
            return parentDr;
        }

        public DiscoveryResponse getDupeParentDr() {
            return dupeParentDr;
        }

        public Target getDupeTarget() {
            return dupeTarget;
        }

        public DiscoveryResponse getDr() {
            return dr;
        }

        public String getDupeAppendage() {
            return dupeAppendage;
        }

        public int getDupeIndex() {
            return dupeIndex;
        }
    }

    private DiscoveryResponse saveDupeDiscoveryResponse(DupeDiscoveryResponseContext ctx) throws Exception {
        String probeType = ctx.getDupeTarget().getProbeInfo().getProbeType();
        switch (probeType) {
            case "Azure Service Principal":
                return saveDupeDiscoveryResponseSP(ctx);
            case "Azure Subscription":
                return saveDupeDiscoveryResponseSubscription(ctx);
            default:
                throw new IllegalArgumentException(
                        "Cannot create duplicate discovery response, target="
                                + ctx.getDupeTarget().getDisplayName() + ", unsupported probe type="
                                + probeType);
        }
    }

    private DiscoveryResponse saveDupeDiscoveryResponseSP(DupeDiscoveryResponseContext ctx) {
        DiscoveryResponse.Builder drBuilder = ctx.getDr().toBuilder();
        DerivedTargetSpecificationDTO dtDto = drBuilder.getDerivedTarget(0);
        DerivedTargetSpecificationDTO.Builder dtBuilder = dtDto.toBuilder();
        List<com.vmturbo.platform.common.dto.Discovery.AccountValue> avList = dtBuilder.getAccountValueList();
        dtBuilder.clearAccountValue();
        for (com.vmturbo.platform.common.dto.Discovery.AccountValue av : avList) {
            String val;
            if (av.getKey().equals("name")) {
                val = ctx.getDupeTarget().getDisplayName();
            } else if (av.getKey().equals("username") || av.getKey().equals("tenant") || av.getKey()
                    .equals("client") || av.getKey().equals("targetCostRelatedFields")) {
                val = av.getStringValue() + ctx.getDupeAppendage();
            } else {
                val = av.getStringValue();
            }
            dtBuilder.addAccountValue(com.vmturbo.platform.common.dto.Discovery.AccountValue.newBuilder()
                    .setKey(av.getKey())
                    .setStringValue(val)
                    .build());
        }
        drBuilder.clearDerivedTarget();
        drBuilder.addDerivedTarget(dtBuilder);
        DiscoveryResponse dupeDr = drBuilder.build();
        String tgId = DiscoveryDumpFilename.sanitize(
                ctx.getDupeTarget().getId()
                        + "_" + ctx.getDupeTarget().getProbeInfo().getProbeType()
                        + "_" + ctx.getDupeTarget().getDisplayName());
        discoveryDumper.dump(tgId, dupeDr, DiscoveryType.FULL);
        return dupeDr;
    }

    private DiscoveryResponse saveDupeDiscoveryResponseSubscription(DupeDiscoveryResponseContext ctx)
            throws ParseException {
        List<String> idsToReplace = new ArrayList<>();
        DiscoveryResponse.Builder drBuilder = ctx.getDr().toBuilder();
        // update entityDTOs
        List<EntityDTO> entities = drBuilder.getEntityDTOList();
        drBuilder.clearEntityDTO();
        List<String> excludedEntityTypes = ctx.getTargetInfo().getExcludedEntityTypes();
        if (excludedEntityTypes != null) {
            entities = entities.stream()
                    .filter(e -> !excludedEntityTypes.contains(e.getEntityType().name()))
                    .collect(Collectors.toList());
        }
        List<EntityDTO> clonedEntities = entities.stream()
                .map(e -> duplicateDto(ctx, idsToReplace, EntityDTO.class, e))
                .collect(Collectors.toList());
        drBuilder.addAllEntityDTO(clonedEntities);
        // update discoveredGroups
        List<GroupDTO> discGroups = drBuilder.getDiscoveredGroupList();
        drBuilder.clearDiscoveredGroup();
        List<GroupDTO> clonedGroups = discGroups.stream()
                .map(e -> duplicateDto(ctx, idsToReplace, GroupDTO.class, e))
                .collect(Collectors.toList());
        drBuilder.addAllDiscoveredGroup(clonedGroups);
        // update derivedTarget
        List<DerivedTargetSpecificationDTO> derivedTargets = drBuilder.getDerivedTargetList();
        drBuilder.clearDerivedTarget();
        List<DerivedTargetSpecificationDTO> clonedDerTargets = derivedTargets.stream()
                .map(e -> duplicateDto(ctx, idsToReplace, DerivedTargetSpecificationDTO.class, e))
                .collect(Collectors.toList());
        drBuilder.addAllDerivedTarget(clonedDerTargets);
        // update nonMarketEntityDTO
        List<NonMarketEntityDTO> nonMarkets = drBuilder.getNonMarketEntityDTOList();
        drBuilder.clearNonMarketEntityDTO();
        List<NonMarketEntityDTO> clonedNonMarkets = nonMarkets.stream()
                .map(e -> duplicateDto(ctx, idsToReplace, NonMarketEntityDTO.class, e))
                .collect(Collectors.toList());
        drBuilder.addAllNonMarketEntityDTO(clonedNonMarkets);
        String tgId = DiscoveryDumpFilename.sanitize(
                ctx.getDupeTarget().getId()
                        + "_" + ctx.getDupeTarget().getProbeInfo().getProbeType()
                        + "_" + ctx.getDupeTarget().getDisplayName());
        DiscoveryResponse dupeDr = drBuilder.build();
        if (idsToReplace.size() > 0) {
            dupeDr = replaceIds(idsToReplace, ctx.getDupeAppendage(), dupeDr);
        }
        DiscoveryResponse drWithExtraEntities = createNewEntities(dupeDr, ctx);
        if (drWithExtraEntities != null) {
            dupeDr = drWithExtraEntities;
        }
        // Standard_D4a_v4, Standard_E32-8ds_v4
        discoveryDumper.dump(tgId, dupeDr, DiscoveryType.FULL);
        return dupeDr;
    }

    private DiscoveryResponse createNewEntities(DiscoveryResponse dupeDr,
            DupeDiscoveryResponseContext ctx) throws ParseException {
        ExtraEntityInfo extraEntityInfo = ctx.getTargetInfo().getExtraEntityInfo();
        if (extraEntityInfo == null || extraEntityInfo.getTotalExtraCopies() == 0) {
            return null;
        }
        DiscoveryResponse.Builder drBuilder = dupeDr.toBuilder();
        List<EntityDTO> entities = drBuilder.getEntityDTOList();
        Pair<EntityDTO, Integer> bizAccountPair = getBusinessAccount(entities);
        EntityDTO.Builder bizAccountBuilder = bizAccountPair.getLeft().toBuilder();
        if (extraEntityInfo.getExtraVmsPerAccount() > 0) {
            int count = extraEntityInfo.getExtraVmsPerAccount();
            if (extraEntityInfo.incrementPerAccount()) {
                count = count + ctx.getDupeIndex();
            }
            createNewEntities(count, drBuilder, EntityType.VIRTUAL_MACHINE, entities, bizAccountBuilder);
        }
        if (extraEntityInfo.getExtraVmSpecsPerAccount() > 0) {
            int count = extraEntityInfo.getExtraVmSpecsPerAccount();
            if (extraEntityInfo.incrementPerAccount()) {
                count = count + ctx.getDupeIndex();
            }
            createNewEntities(count, drBuilder, EntityType.VIRTUAL_MACHINE_SPEC, entities, bizAccountBuilder);
        }
        if (extraEntityInfo.getExtraResourceGroupsPerAccount() > 0) {
            int count = extraEntityInfo.getExtraResourceGroupsPerAccount();
            if (extraEntityInfo.incrementPerAccount()) {
                count = count + ctx.getDupeIndex();
            }
            createNewResourceGroups(count, drBuilder, ctx);
        }
        drBuilder.removeEntityDTO(bizAccountPair.getRight());
        drBuilder.addEntityDTO(bizAccountPair.getRight(), bizAccountBuilder.build());
        return drBuilder.build();
    }

    private void createNewEntities(int count,
            DiscoveryResponse.Builder drBuilder,
            EntityType entityType,
            List<EntityDTO> allEntities,
            EntityDTO.Builder bizAccountBuilder) throws ParseException {
        List<EntityDTO> entsToCopy = getEntitiesToCopy(allEntities, count, e -> e.getEntityType() == entityType);
        int index = 0;
        int iteration = 0;
        while (index < count) {
            iteration++;
            for (EntityDTO entToCopy : entsToCopy) {
                index++;
                EntityDTO newEntity;
                switch (entityType) {
                    case VIRTUAL_MACHINE:
                        newEntity = createNewVm(entToCopy, iteration);
                        break;
                    case VIRTUAL_MACHINE_SPEC:
                        newEntity = createNewVmSpec(entToCopy, iteration);
                        break;
                    default:
                        throw new IllegalStateException("No support for adding copies of entities of type " + entityType.name());
                }
                drBuilder.addEntityDTO(newEntity);
                addToBusinessAccount(bizAccountBuilder, newEntity);
                addToResourceGroups(drBuilder, entToCopy, newEntity);
                if (index == count) {
                    break;
                }
            }
        }
    }

    /**
     * Creates a resource group like the following.
     *
     * <p><pre><code>
     * discoveredGroup {
     *     entity_type: UNKNOWN
     *     display_name: "[name]"
     *     group_name: "/subscriptions/[subscription]/resourceGroups/[name]"
     *     member_list {
     *     }
     *     groupType: RESOURCE
     *     owner: "[subscription]"
     * }
     * </code></pre></p>
     * @param count the count
     * @param drBuilder the builder.
     * @param ctx the context.
     */
    private void createNewResourceGroups(int count, DiscoveryResponse.Builder drBuilder, DupeDiscoveryResponseContext ctx) {
        String subId = getTargetSubscriptionId(ctx.getDupeParentDr());
        String groupNameTemplate = "/subscriptions/" + subId + "/resourceGroups/%s";
        for (int i = 0; i < count; i++) {
            String displayName = "group_" + (i + 1) + ctx.getDupeAppendage();
            String groupName = String.format(groupNameTemplate, displayName);
            GroupDTO.Builder gBuilder = GroupDTO.newBuilder();
            gBuilder.setEntityType(EntityType.UNKNOWN);
            gBuilder.setDisplayName(displayName);
            gBuilder.setGroupName(groupName);
            gBuilder.setGroupType(GroupType.RESOURCE);
            gBuilder.setOwner(subId);
            MembersList.Builder mBuilder = MembersList.newBuilder();
            gBuilder.setMemberList(mBuilder.build());
            drBuilder.addDiscoveredGroup(gBuilder.build());
        }
    }

    /**
     * Finds the business account entity given a list of entities.
     * @param entities all entities.
     * @return The business account entity and its index.
     */
    private Pair<EntityDTO, Integer> getBusinessAccount(List<EntityDTO> entities) {
        int index = -1;
        for (EntityDTO entity : entities) {
            index++;
            if (entity.getEntityType() == EntityType.BUSINESS_ACCOUNT) {
                return Pair.of(entity, index);
            }
        }
        throw new IllegalStateException("The discovery response is missing a BUSINESS_ACCOUNT entity");
    }

    private List<Pair<GroupDTO, Integer>> getDiscoveredGroups(DiscoveryResponse.Builder drBuilder,
            EntityDTO entity) {
        List<Pair<GroupDTO, Integer>> discoveredGroups = new ArrayList<>();
        List<GroupDTO> groups = drBuilder.getDiscoveredGroupList();
        int index = -1;
        for (GroupDTO group : groups) {
            index++;
            MembersList members = group.getMemberList();
            Optional<String> match = members.getMemberList().stream().filter(s -> s.equals(entity.getId())).findFirst();
            if (match.isPresent()) {
                discoveredGroups.add(Pair.of(group, index));
            }
        }
        return discoveredGroups;
    }

    private EntityDTO createNewVmSpec(EntityDTO orig, int iteration) throws ParseException {
        return createNewEntity(orig, iteration);
    }

    private EntityDTO createNewVm(EntityDTO orig, int iteration) throws ParseException {
        return createNewEntity(orig, iteration);
    }

    private EntityDTO createNewEntity(EntityDTO orig, int iteration) throws ParseException {
        EntityDTO.Builder newEntity = orig.toBuilder();
        String appendage = "_copy_" + iteration;
        String newName = orig.getDisplayName() + appendage;
        String newId = orig.getId() + appendage;
        newEntity.setDisplayName(newName);
        EntityDTO ent = newEntity.build();
        String s = ent.toString().replaceAll(orig.getId(), newId);
        return TextFormat.parse(s, EntityDTO.class);
    }

    private void addToBusinessAccount(EntityDTO.Builder bizAccountBuilder, EntityDTO entity) {
        // add to business account:
        //  connectedEntities {
        //    connectedEntityId: "/subscriptions/9224a246-c20a-4868-a641-a32971a69f92_dupe_1/resourceGroups/Cloud-PaaS-RG/providers/Microsoft.Web/serverfarms/S1-Windows-EastUS_dupe_1"
        //    connectionType: OWNS_CONNECTION
        //  }
        bizAccountBuilder.addConnectedEntities(ConnectedEntity.newBuilder()
                .setConnectedEntityId(entity.getId())
                .setConnectionType(ConnectionType.OWNS_CONNECTION)
                .build());
    }

    private void addToResourceGroups(DiscoveryResponse.Builder drBuilder, EntityDTO origEntity, EntityDTO newEntity) {
        // discoveredGroup {
        //     entity_type: UNKNOWN
        //     display_name: "creichen-rg-OM88585"
        //     group_name: "/subscriptions/9224a246-c20a-4868-a641-a32971a69f92_dupe_1/resourceGroups/creichen-rg-OM88585"
        //     member_list {
        //         member: "/subscriptions/9224a246-c20a-4868-a641-a32971a69f92_dupe_1/resourceGroups/creichen-rg-OM88585/providers/Microsoft.Web/serverfarms/creichen-asp-OM88585-EP1-withfunapp-windows_dupe_1"
        //         member: "/subscriptions/9224a246-c20a-4868-a641-a32971a69f92_dupe_1/resourceGroups/creichen-rg-OM88585/providers/Microsoft.Web/serverfarms/creichen-asp-OM88585-EP1-nofunapp-linux_dupe_1"
        //         member: "/subscriptions/9224a246-c20a-4868-a641-a32971a69f92_dupe_1/resourceGroups/creichen-rg-OM88585/providers/Microsoft.Web/serverfarms/creichen-asp-OM88585-S1-withfunapp-linux_dupe_1"
        //         member: "/subscriptions/9224a246-c20a-4868-a641-a32971a69f92_dupe_1/resourceGroups/creichen-rg-OM88585/providers/Microsoft.Web/serverfarms/creichen-asp-OM88585-W1-nologicapp-windows_dupe_1"
        //         member: "/subscriptions/9224a246-c20a-4868-a641-a32971a69f92_dupe_1/resourceGroups/creichen-rg-OM88585/providers/Microsoft.Web/serverfarms/creichen-asp-OM88585-EP1-nofunapp-windows_dupe_1"
        //         member: "/subscriptions/9224a246-c20a-4868-a641-a32971a69f92_dupe_1/resourceGroups/creichen-rg-OM88585/providers/Microsoft.Web/serverfarms/creichen-asp-OM88585-W1-nologicapp-linux_dupe_1"
        //     }
        //     groupType: RESOURCE
        //     owner: "9224a246-c20a-4868-a641-a32971a69f92_dupe_1"
        // }
        List<Pair<GroupDTO, Integer>> dgPairs = getDiscoveredGroups(drBuilder, origEntity);
        if (dgPairs.size() == 0) {
            throw new IllegalStateException("No discoveredGroup not found for entity: " + origEntity.getId());
        }
        for (Pair<GroupDTO, Integer> dgPair : dgPairs) {
            GroupDTO discoveredGroup = dgPair.getLeft();
            MembersList.Builder membersBuilder = discoveredGroup.getMemberList().toBuilder();
            membersBuilder.addMember(newEntity.getId());
            GroupDTO.Builder groupBuilder = discoveredGroup.toBuilder();
            groupBuilder.clearMembers();
            groupBuilder.setMemberList(membersBuilder.build());
            drBuilder.removeDiscoveredGroup(dgPair.getRight());
            drBuilder.addDiscoveredGroup(dgPair.getRight(), groupBuilder.build());
        }
    }

    private List<EntityDTO> getEntitiesToCopy(List<EntityDTO> entities, int count, Predicate<EntityDTO> predicate) {
        List<EntityDTO> filtered = entities.stream().filter(predicate).collect(Collectors.toList());
        if (filtered.size() < count) {
            return filtered;
        }
        return filtered.subList(0, count);
    }

    private DiscoveryResponse replaceIds(List<String> idsToReplace, String dupeAppendage, DiscoveryResponse dupeDr)
            throws ParseException {
        String s = dupeDr.toString();
        for (String idToReplace : idsToReplace) {
            s = s.replaceAll(idToReplace, idToReplace + dupeAppendage);
        }
        return TextFormat.parse(s, DiscoveryResponse.class);
    }

    private <T extends Message> T duplicateDto(DupeDiscoveryResponseContext ctx,
            List<String> idsToReplace,
            Class<T> msgClass,
            Message dtoMsg) {
        String s = dtoMsg.toString();
        s = replaceAccountIds(ctx, s);
        try {
            s = replaceTargetName(ctx, s);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to replace target name for entity: " + dtoMsg.toString(), e);
        }
        try {
            T dto = TextFormat.parse(s, msgClass);
            if (EntityDTO.class == msgClass) {
                dto = (T)duplicateEntityDTO((EntityDTO)dto, ctx.getDupeAppendage(), idsToReplace);
            }
            return dto;
        } catch (ParseException e) {
            throw new IllegalStateException("Failed to parse entity: " + dtoMsg.toString(), e);
        }
    }

    private EntityDTO duplicateEntityDTO(EntityDTO entityDTO, String dupeAppendage, List<String> idsToReplace) {
        // Don't append to the compute tier display names
        boolean replaceDisplayName;
        boolean replaceId;
        switch (entityDTO.getEntityType()) {
            case VIRTUAL_MACHINE_SPEC:
            case DATABASE:
            case APPLICATION_COMPONENT_SPEC:
            case APPLICATION_COMPONENT:
            case VIRTUAL_MACHINE:
            case VIRTUAL_VOLUME:
                replaceDisplayName = true;
                replaceId = true;
                break;
            case DATABASE_TIER:
            case COMPUTE_TIER:
            case STORAGE_TIER:
            case REGION:
            case CLOUD_SERVICE:
            case SERVICE_PROVIDER:
            case BUSINESS_ACCOUNT: // Business account gets updated with replaceAll for subscription ID and target name
                replaceDisplayName = false;
                replaceId = false;
                break;
            default:
                // If duplicating targets, want to make sure we know whether to replace the IDs & display names.
                // If an entity comes along in a dump that I'm not sure about, force the dev to decide rather than guess.
                throw new IllegalStateException("I don't know how to duplicate entity type: " + entityDTO.getEntityType().name());
        }
        if (replaceId) {
            idsToReplace.add(entityDTO.getId());
        }
        if (replaceDisplayName) {
            EntityDTO.Builder eBuilder = entityDTO.toBuilder();
            eBuilder.setDisplayName(eBuilder.getDisplayName() + dupeAppendage);
            return eBuilder.build();
        }
        return entityDTO;
    }

    private String replaceAccountIds(DupeDiscoveryResponseContext ctx, String dtoString) {
        String origSubId = getTargetSubscriptionId(ctx.getParentDr());
        String dupeSubId = getTargetSubscriptionId(ctx.getDupeParentDr());
        dtoString = dtoString.replaceAll(origSubId, dupeSubId);
        String origTenantId = ctx.getTargetInfo().getProperty("tenant");
        String dupeTenantId = origTenantId + ctx.getDupeAppendage();
        dtoString = dtoString.replaceAll(origTenantId, dupeTenantId);
        String origClientId = ctx.getTargetInfo().getProperty("client");
        String dupeClientId = origClientId + ctx.getDupeAppendage();
        return dtoString.replaceAll(origClientId, dupeClientId);
    }

    private String replaceTargetName(DupeDiscoveryResponseContext ctx, String dtoString) throws Exception {
        // replace the account name (e.g. EA - PT2 sould be EA - PT2_dupe_1)
        // We can't just blanket replace because some items like BUSINESS_ACCOUNT are already
        // using the dupe appendage and would otherwise get 2 dupe appendages
        StringBuilder sb = new StringBuilder();
        String origName = getTargetName(ctx.getParentDr());
        String dupeName = getTargetName(ctx.getDupeParentDr());
        try (BufferedReader reader = new BufferedReader(new StringReader(dtoString))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.indexOf(origName) > -1 && line.indexOf(dupeName) == -1) {
                    line = line.replaceAll(origName, dupeName);
                }
                sb.append(line);
                sb.append(System.getProperty("line.separator"));
            }
        }
        return sb.toString();
    }

    private Optional<Target> getExistingTarget(TargetSpec tSpec) {
        Optional<ProbeInfo> probeInfo = probeStore.getProbe(tSpec.getProbeId());
        if (!probeInfo.isPresent()) {
            return Optional.empty();
        }
        List<Target> allTargets = targetStore.getAll();
        String targetName = getTargetName(tSpec);
        return getTarget(allTargets, probeInfo.get().getProbeType(), targetName);
    }

    private TargetInfo duplicateTargetInfo(TargetInfo tInfo, String dupeAppendage) {
        TargetInfo copy = new TargetInfo();
        copy.setName(tInfo.getName() + dupeAppendage);
        copy.setType(tInfo.getType());
        Map<String, String> props = tInfo.getProperties();
        Map<String, String> propsCopy = new HashMap(props);
        propsCopy.put("tenant", props.get("tenant") + dupeAppendage);
        propsCopy.put("client", props.get("client") + dupeAppendage);
        copy.setProperties(propsCopy);
        return copy;
    }

    private void duplicateDerivedTargets(TargetInfo targetInfo,
            String dupeAppendage,
            Target dupeParentTarget,
            DiscoveryResponse parentDr,
            DiscoveryResponse dupeParentDr,
            List<DerivedTargetInfo> dtInfos,
            int dupeCount) throws Exception {
        List<DerivedTargetSpecificationDTO> dupeDerivedTargetSpecs = dupeParentDr.getDerivedTargetList();
        List<DerivedTargetSpecificationDTO> neededDerivedTargets = new ArrayList<>();
        List<Target> allTargets = targetStore.getAll();
        for (DerivedTargetSpecificationDTO dupeDerivedTargetSpec : dupeDerivedTargetSpecs) {
            Optional<Target> t = getTarget(allTargets, dupeDerivedTargetSpec.getProbeType(), getTargetName(dupeDerivedTargetSpec));
            if (!t.isPresent()) {
                neededDerivedTargets.add(dupeDerivedTargetSpec);
            }
        }
        // First we create any needed derived targets in the target store
        if (neededDerivedTargets.size() > 0) {
            derivedTargetParser.instantiateDerivedTargets(dupeParentTarget.getId(), neededDerivedTargets);
            // refresh all targets
            allTargets = targetStore.getAll();
        }
        Map<String, DerivedTargetInfo> dtInfoMap = dtInfos.stream().collect(
                Collectors.toMap(dti -> dti.getDerivedTarget().getDisplayName(),
                        Function.identity()));
        // Next we copy the discovery responses of the derived targets into the TP cache if needed
        for (DerivedTargetSpecificationDTO dupeDerivedTargetSpec : dupeDerivedTargetSpecs) {
            String dupeDerivedTargetName = getTargetName(dupeDerivedTargetSpec);
            Optional<Target> dupeTarget = getTarget(allTargets, dupeDerivedTargetSpec.getProbeType(), dupeDerivedTargetName);
            if (!dupeTarget.isPresent()) {
                throw new IllegalStateException(
                        "Expected dupe derived target to exist: " + dupeDerivedTargetName);
            }
            String origDerivedTargetName = dupeDerivedTargetName.substring(0, dupeDerivedTargetName.lastIndexOf(DUPE_APPENDAGE));
            DerivedTargetInfo dtInfo = dtInfoMap.get(origDerivedTargetName);
            String sanitizedName = DiscoveryDumpFilename.sanitize(dupeDerivedTargetSpec.getProbeType() + "_" + dupeDerivedTargetName);
            File cacheDrFile = getCachedDiscoveryResponseFile(discoveryDumper.getDumpDirectory(),
                    sanitizedName, null);
            DiscoveryResponse dupeDr;
            if (cacheDrFile == null) {
                DupeDiscoveryResponseContext ctx = new DupeDiscoveryResponseContext(targetInfo,
                        parentDr, dupeParentDr, dupeTarget.get(), dtInfo.getDerivedDr(),
                        dupeAppendage, dupeCount);
                dupeDr = saveDupeDiscoveryResponse(ctx);
            }
//            else {
//                DiscoveryDumpFilename ddf = DiscoveryDumpFilename.parse(cacheDrFile.getName());
//                dupeDr = discoveryDumper.getDiscoveryResponse(ddf);
//            }
            // Don't really need to create the Azure Wasted Volumes derived target, and it will just
            // clutter up the target settings page.  The standard MS-AZR-0003P cost target also
            // already exists.  So we don't need to create derived targets of derived targets r/n.
            // duplicateDerivedTargets(targetInfo, dupeAppendage, dupeTarget.get(),
            //        dtInfo.getDerivedDr(), dupeDr, dtInfo.getDerivedTargets(), dupeCount);
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

    private String getTargetName(DiscoveryResponse svcPrincipalDr) {
        List<DerivedTargetSpecificationDTO> specs = svcPrincipalDr.getDerivedTargetList();
        if (specs.size() != 1) {
            throw new IllegalStateException("Expected a service principal Discovery Response "
                    + "with a single derived target");
        }
        return getTargetName(specs.get(0));
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

    private String getTargetSubscriptionId(DiscoveryResponse svcPrincipalDr) {
        List<DerivedTargetSpecificationDTO> specs = svcPrincipalDr.getDerivedTargetList();
        if (specs.size() != 1) {
            throw new IllegalStateException("Expected a service principal Discovery Response "
                    + "with a single derived target");
        }
        DerivedTargetSpecificationDTO dts = specs.get(0);
        List<com.vmturbo.platform.common.dto.Discovery.AccountValue> accountVals =
                dts.getAccountValueList().stream()
                        .filter(av -> av.getKey().equals(ACCOUNT_USERNAME))
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
    private List<DerivedTargetInfo> preCreateDerivedTargets(TargetInfo targetInfo,
            File targetPrecacheDir,
            Target parentTarget,
            DiscoveryResponse parentDiscoveryResp) {
        List<DerivedTargetInfo> derivedTargetInfos = new ArrayList<>();
        List<DerivedTargetSpecificationDTO> derivedTargetSpecs = parentDiscoveryResp.getDerivedTargetList();
        if (derivedTargetSpecs == null || derivedTargetSpecs.size() == 0) {
            logger.debug("No derived targets found for target {}, type={}",
                    parentTarget.getDisplayName(), parentTarget.getProbeInfo().getProbeType());
            return Collections.emptyList();
        }

        logger.info("Processing derived targets of target {}, type={}, count={}",
                parentTarget.getDisplayName(), parentTarget.getProbeInfo().getProbeType(),
                derivedTargetSpecs.size());
        List<DerivedTargetSpecificationDTO> neededDerivedTargets = new ArrayList<>();
        List<Target> allTargets = targetStore.getAll();
        for (DerivedTargetSpecificationDTO derivedTarget : derivedTargetSpecs) {
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
                List<DerivedTargetInfo> childDerivedTargets = preCreateDerivedTargets(targetInfo,
                        targetPrecacheDir, derivedTarget, derivedDr);
                DerivedTargetInfo derivedTargetInfo = new DerivedTargetInfo(derivedTarget,
                        derivedDr, childDerivedTargets);
                derivedTargetInfos.add(derivedTargetInfo);
            } else {
                logger.info("No cached discovery response found for derived target {}, type={}",
                        derivedTarget.getDisplayName(), derivedTarget.getProbeInfo().getProbeType());
            }
        }
        return derivedTargetInfos;
    }

    /**
     * An encapsulation of derived target info.
     */
    private class DerivedTargetInfo {
        final Target derivedTarget;
        final DiscoveryResponse derivedDr;
        final List<DerivedTargetInfo> derivedTargets;

        DerivedTargetInfo(Target derivedTarget, DiscoveryResponse derivedDr,
                List<DerivedTargetInfo> derivedTargets) {
            this.derivedTarget = derivedTarget;
            this.derivedDr = derivedDr;
            this.derivedTargets = derivedTargets;
        }

        public Target getDerivedTarget() {
            return derivedTarget;
        }

        public String getProbeType() {
            return derivedTarget.getProbeInfo().getProbeType();
        }

        public DiscoveryResponse getDerivedDr() {
            return derivedDr;
        }

        public List<DerivedTargetInfo> getDerivedTargets() {
            return derivedTargets;
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
        private int copies;
        private ExtraEntityInfo extraEntityInfo;
        private List<String> excludedEntityTypes;
        private Map<String, String> props = new HashMap<>();

        String getName() {
            return name;
        }

        void setName(String name) {
            this.name = name;
        }

        String getType() {
            return type;
        }

        void setType(String type) {
            this.type = type;
        }

        boolean isEnabled() {
            return enabled;
        }

        int getCopies() {
            return copies;
        }

        Map<String, String> getProperties() {
            return props;
        }

        void setProperties(Map<String, String>  props) {
            this.props = props;
        }

        String getProperty(String name) {
            return props.get(name);
        }

        public List<String> getExcludedEntityTypes() {
            return excludedEntityTypes;
        }

        public void setExcludedEntityTypes(List<String> excludedEntityTypes) {
            this.excludedEntityTypes = excludedEntityTypes;
        }

        ExtraEntityInfo getExtraEntityInfo() {
            return extraEntityInfo;
        }

        void setExtraEntityInfo(ExtraEntityInfo extraEntityInfo) {
            this.extraEntityInfo = extraEntityInfo;
        }
    }

    /**
     * Contains info about creation of extra entities.
     */
    private static class ExtraEntityInfo {
        boolean incrementPerAccount;
        private int extraVmsPerAccount;
        private int extraVmSpecsPerAccount;
        private int extraResourceGroupsPerAccount;

        public boolean incrementPerAccount() {
            return incrementPerAccount;
        }

        public void setIncrementPerAccount(boolean incrementPerAccount) {
            this.incrementPerAccount = incrementPerAccount;
        }

        int getExtraVmsPerAccount() {
            return extraVmsPerAccount;
        }

        void setExtraVmsPerAccount(int extraVmsPerAccount) {
            this.extraVmsPerAccount = extraVmsPerAccount;
        }

        int getExtraVmSpecsPerAccount() {
            return extraVmSpecsPerAccount;
        }

        void setExtraVmSpecsPerAccount(int extraVmSpecsPerAccount) {
            this.extraVmSpecsPerAccount = extraVmSpecsPerAccount;
        }

        public int getExtraResourceGroupsPerAccount() {
            return extraResourceGroupsPerAccount;
        }

        public void setExtraResourceGroupsPerAccount(int extraResourceGroupsPerAccount) {
            this.extraResourceGroupsPerAccount = extraResourceGroupsPerAccount;
        }

        int getTotalExtraCopies() {
            return extraVmsPerAccount + extraVmSpecsPerAccount + extraResourceGroupsPerAccount;
        }
    }
}

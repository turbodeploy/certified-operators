package com.vmturbo.topology.processor.diagnostics;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.RecursiveZipReader;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.topology.processor.TopologyProcessorComponent;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetInfo;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.IdentifiedEntityDTO;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetDeserializationException;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Handle diagnostics for {@link TopologyProcessorComponent}.
 * The topology processor dump is made of a zip file that contains one file for the registered
 * targets, one file for their schedules, and then one file per target with the entities
 * associated with that target.
 * TODO: add the probes
 */
public class TopologyProcessorDiagnosticsHandler {

    private static final String TARGETS_DIAGS_FILE_NAME = "Targets.diags";
    private static final String SCHEDULES_DIAGS_FILE_NAME = "Schedules.diags";
    private static final String ID_DIAGS_FILE_NAME = "Identity.diags";

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final TargetStore targetStore;
    private final Scheduler scheduler;
    private final EntityStore entityStore;
    private final IdentityProvider identityProvider;
    private final DiagnosticsWriter diagnosticsWriter;

    private final Logger logger = LogManager.getLogger();

    public TopologyProcessorDiagnosticsHandler(
            @Nonnull final TargetStore targetStore,
            @Nonnull final Scheduler scheduler,
            @Nonnull final EntityStore entityStore,
            @Nonnull final IdentityProvider identityProvider,
            @Nonnull final DiagnosticsWriter diagnosticsWriter) {
        this.targetStore = targetStore;
        this.scheduler = scheduler;
        this.entityStore = entityStore;
        this.identityProvider = identityProvider;
        this.diagnosticsWriter = diagnosticsWriter;
    }

    /**
     * Dump TopologyProcessor diagnostics.
     *
     * @param diagnosticZip the ZipOutputStream to dump diags to
     */
    public void dumpDiags(ZipOutputStream diagnosticZip) {
        // Targets
        List<Target> targets = targetStore.getAll();
        diagnosticsWriter.writeZipEntry(TARGETS_DIAGS_FILE_NAME,
                targets.stream()
                .map(Target::getNoSecretDto)
                .map(GSON::toJson)
                .collect(Collectors.toList()),
            diagnosticZip);

        // Schedules
        diagnosticsWriter.writeZipEntry(SCHEDULES_DIAGS_FILE_NAME,
                targets.stream()
                .map(Target::getId)
                .map(scheduler::getDiscoverySchedule)
                .filter(Optional::isPresent).map(Optional::get)
                .map(schedule -> GSON.toJson(schedule, TargetDiscoverySchedule.class))
                .collect(Collectors.toList()),
            diagnosticZip);

        // Entities (one file per target)
        for (Target target : targets) {
            // TODO(Shai): add tartgetId to the content of the file (not just part of the name)
            diagnosticsWriter.writeZipEntry("Entities." + target.getId() + ".diags",
                entityStore.discoveredByTarget(target.getId()).entrySet().stream()
                .map(entry -> new IdentifiedEntityDTO(entry.getKey(), entry.getValue()))
                .map(IdentifiedEntityDTO::toJson)
                .collect(Collectors.toList()),
                diagnosticZip);
        }

        diagnosticsWriter.writeZipEntry(ID_DIAGS_FILE_NAME,
                identityProvider.collectDiags(),
                diagnosticZip);
    }

    /**
     * Dump TopologyProcessor anonymized diagnostics.
     *
     * @param diagnosticZip the ZipOutputStream to dump diags to
     */
    public void dumpAnonymizedDiags(ZipOutputStream diagnosticZip) {
        // Targets
        List<Target> targets = targetStore.getAll();
        diagnosticsWriter.writeZipEntry(TARGETS_DIAGS_FILE_NAME,
                                         targets.stream()
                                                .map(Target::getNoSecretAnonymousDto)
                                                .map(GSON::toJson)
                                                .collect(Collectors.toList()),
                                         diagnosticZip);

        // Schedules
        diagnosticsWriter.writeZipEntry(SCHEDULES_DIAGS_FILE_NAME,
                                         targets.stream()
                                                .map(Target::getId)
                                                .map(scheduler::getDiscoverySchedule)
                                                .filter(Optional::isPresent).map(Optional::get)
                                                .map(schedule -> GSON.toJson(schedule, TargetDiscoverySchedule.class))
                                                .collect(Collectors.toList()),
                                         diagnosticZip);

        // Entities (one file per target)
        for (Target target : targets) {
            // TODO(Shai): add tartgetId to the content of the file (not just part of the name)
            diagnosticsWriter.writeZipEntry("Entities." + target.getId() + ".diags",
                                             entityStore.discoveredByTarget(target.getId()).entrySet().stream()
                                                        .map(entry -> new IdentifiedEntityDTO(entry.getKey(), entry.getValue()))
                                                        .map(IdentifiedEntityDTO::toJsonAnonymized)
                                                        .collect(Collectors.toList()),
                                             diagnosticZip);
        }

        diagnosticsWriter.writeZipEntry(ID_DIAGS_FILE_NAME,
                identityProvider.collectDiags(),
                diagnosticZip);
    }

    private static final Pattern ENTITIES_PATTERN = Pattern.compile("Entities\\.(\\d*)\\.diags");

    /**
     * Restore topology processor diagnostics from a {@link ZipInputStream}. Look for the targets file,
     * schedules, and for entities files (one for each target) and restore them.
     *
     * @param zis the input stream with compressed diagnostics
     * @return list of restored targets
     * @throws TargetDeserializationException when one target or more cannot be serialized
     * @throws InvalidTargetException when the target is invalid
     * @throws IOException when there is a problem with I/O
     */
    public List<Target> restore(InputStream zis)
            throws IOException, TargetDeserializationException,
                InvalidTargetException {
        for (Diags diags : new RecursiveZipReader(zis)) {
            String diagsName = diags.getName();
            if (TARGETS_DIAGS_FILE_NAME.equals(diagsName)) {
                restoreTargets(diags.getLines());
            } else if (SCHEDULES_DIAGS_FILE_NAME.equals(diagsName)) {
                restoreSchedules(diags.getLines());
            } else if (ID_DIAGS_FILE_NAME.equals(diagsName)) {
                try {
                    identityProvider.restoreDiags(diags.getLines());
                } catch (RuntimeException e) {
                    logger.error("Failed to restore Identity diags.", e);
                }
            } else {
                Matcher m = ENTITIES_PATTERN.matcher(diagsName);
                if (m.matches()) {
                    long targetId = Long.valueOf(m.group(1));
                    restoreEntities(targetId, diags.getLines());
                }
            }
        }
        return targetStore.getAll();
    }

    /**
     * Clear the target store and re-populate it based on a list of serialized targets.
     *
     * @param serializedTargets a list of json-serialized targets
     * @throws IOException if there is a problem reading the file
     * @throws TargetDeserializationException if deserialization fails
     */
    public void restoreTargets(List<String> serializedTargets)
            throws TargetDeserializationException, IOException {
        logger.info("Attempting to restore " + serializedTargets.size() + " targets");
        targetStore.removeAllTargets();
        final long restoredTargetsCount = serializedTargets.stream()
            .map(serializedTarget -> {
                try {
                    return GSON.fromJson(serializedTarget, TargetInfo.class);
                } catch (JsonParseException e) {
                    logger.error("Error deserializing target. Error: {}, Target: {}",
                            e.toString(), serializedTarget);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .map(targetInfo -> {
                try {
                    return targetStore.createTarget(targetInfo.getId(), targetInfo.getSpec());
                } catch (InvalidTargetException e) {
                    // This shouldn't happen, because the createTarget method we use
                    // here shouldn't do validation.
                    logger.error("Unexpected invalid target exception {} for spec: {}",
                            e.getMessage(), targetInfo.getSpec());
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .count();
        logger.info("Restored {} targets.", restoredTargetsCount);
    }

    /**
     * Restore target discovery schedules from a list of serialized schedules, usually produced
     * via a {@link #dumpDiags(ZipOutputStream) operation.
     *
     * @param serializedSchedules a list of json-serialized shcedules
     * @throws IOException if there is a problem reading the file
     */
    public void restoreSchedules(List<String> serializedSchedules) throws IOException {
        logger.info("Restoring " + serializedSchedules.size() + " schedules");
        Gson gson = new Gson();
        for (String json : serializedSchedules) {
            DiscoverySchedule shcedule = gson.fromJson(json, DiscoverySchedule.class);
                try {
                    scheduler.setDiscoverySchedule(
                            shcedule.getTargetId(),
                            shcedule.getScheduleIntervalMillis(),
                            TimeUnit.MILLISECONDS);
                } catch (TargetNotFoundException e) {
                    logger.warn("While deserializing schedules, target id " + shcedule.getTargetId() + " not found");
                }
        }
        logger.info("Done restoring schedules");
    }

    /**
     * Restore entity DTOs for a given target from a list of serialized entities, usually produced
     * via a {@link #dumpDiags(ZipOutputStream) operation.
     *
     * @param targetId the target ID that the restored entities will be associated with in the {@link EntityStore}
     * @param serializedEntities a list of serialized entity DTOs
     * @throws IOException if there is a problem reading the file
     * @see {@link EntityStore#entitiesDiscovered(long, long, List)
     */
    public void restoreEntities(long targetId, List<String> serializedEntities) throws IOException {
        logger.info("Restoring " + serializedEntities.size() + " entities for target " + targetId);
        Map<Long, EntityDTO> entitiesMap = Maps.newHashMap();
        serializedEntities.stream()
            .map(IdentifiedEntityDTO::fromJson)
            .forEach(dto -> entitiesMap.put(Long.valueOf(dto.getOid()), dto.getEntity()));
        entityStore.entitiesRestored(targetId, entitiesMap);
        logger.info("Done Restoring entities for target " + targetId);
    }

    /**
     * Internal class used for deserializing schedules. Only care about target ID and the interval.
     */
    private static class DiscoverySchedule {
        long targetId;
        long scheduleIntervalMillis;

        public long getTargetId() {
            return targetId;
        }

        public long getScheduleIntervalMillis() {
            return scheduleIntervalMillis;
        }
    }
}

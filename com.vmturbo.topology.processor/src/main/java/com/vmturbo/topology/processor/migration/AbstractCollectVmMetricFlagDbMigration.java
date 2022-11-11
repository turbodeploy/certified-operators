package com.vmturbo.topology.processor.migration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration;
import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Abstract migration class to update database natvie target info and probe info.
 * It removes a new boolean flag `collectVmMetrics` that is responsible for
 * enabling/disabling the collection of virtual machines metrics.
 */

public class AbstractCollectVmMetricFlagDbMigration extends AbstractCollectVmMetricFlagMigration {

    private final Logger logger = LogManager.getLogger();

    private final String propertyName;

    /**
     * Constructor.
     *
     * @param targetStore  target store.
     * @param probeStore   probe store.
     * @param probeType probeType
     */
    protected AbstractCollectVmMetricFlagDbMigration(TargetStore targetStore, ProbeStore probeStore, String probeType, String propertyName) {
        super(targetStore, probeStore, probeType);
        this.propertyName = propertyName;
    }

    @Override
    protected MigrationProgressInfo doStartMigration() {
        Set<MigrationProgressInfo> migrationProgressInfos = new HashSet<>();
            try {
                probeStore.initialize();
                targetStore.initialize();
                updateProbeInfo(probeType);
                updateTargetInfo(probeType);
            } catch (RequiresDataInitialization.InitializationException e) {
                final String msg = "Failed to initialize a store with error: " + e;
                migrationProgressInfos.add(updateMigrationProgress(Migration.MigrationStatus.FAILED, 0, msg));
            } catch (ProbeException e) {
                final String msg = "Cannot update " + probeType + " ProbeInfo: " + e;
                migrationProgressInfos.add(updateMigrationProgress(Migration.MigrationStatus.FAILED, 0, msg));
            } catch (InvalidTargetException | TargetNotFoundException
                     | IdentityStoreException | IdentifierConflictException e) {
                final String msg = "Cannot update " + probeType + " TargetInfo: " + e;
                migrationProgressInfos.add(updateMigrationProgress(Migration.MigrationStatus.FAILED, 0, msg));
            }

        if (!migrationProgressInfos.isEmpty()) {
            return migrationProgressInfos.iterator().next();
        }
        return migrationSucceeded();
    }

    protected void updateTargetInfo(String probeType)
            throws InvalidTargetException, TargetNotFoundException, IdentityStoreException,
            IdentifierConflictException, ProbeException {
        logger.debug(probeType + " migration: update target info");
        final Optional<Long> probeIdOpt = probeStore.getProbeIdForType(probeType);
        if (probeIdOpt.isPresent()) {
            logger.debug("{} migration: probe id exists: {}", probeType, probeIdOpt.get());
            for (Target target : targetStore.getProbeTargets(probeIdOpt.get())) {
                logger.debug("{} migration: update target id {}", probeType, target.getId());
                if (hasVmMetricFlag(target)) {
                        logger.debug("{} migration: target id does have flag {}", probeType, target.getId());
                        final long id = target.getId();
                        final List<TopologyProcessorDTO.AccountValue> accountValues
                                = new ArrayList<>(target.getSpec()
                                .getAccountValueList().stream()
                                .filter(x -> !x.getKey().equals(propertyName))
                                .collect(Collectors.toList()));
                        final TopologyProcessorDTO.TargetSpec targetSpec = target.getSpec()
                                .toBuilder()
                                .clearAccountValue()
                                .addAllAccountValue(accountValues)
                                .build();
                        targetStore.restoreTarget(id, targetSpec);
                        targetStore.updateTarget(id, Collections.emptySet(), Optional.empty(),
                                target.getNoSecretDto().getSpec().getLastEditingUser());
                    }
            }
        } else {
            logger.error(probeType + " migration: probe id is not found in Probe Store");
            throw new ProbeException("Probe id is not found in Probe Store");
        }
    }

    protected void updateProbeInfo(String probeType) throws ProbeException {
        final Optional<MediationMessage.ProbeInfo> infoOpt = probeStore.getProbeInfoForType(probeType);
        if (infoOpt.isPresent()) {
            final MediationMessage.ProbeInfo probeInfo = infoOpt.get();
                if (hasVmMetricFlag(probeInfo)) {
                    List<Discovery.AccountDefEntry> accountDefinitionList = probeInfo.getAccountDefinitionList().stream()
                            .filter(x -> !x.getCustomDefinition().getName().equals(propertyName))
                            .collect(Collectors.toList());

                    probeStore.updateProbeInfo(
                            probeInfo.toBuilder()
                                     .clearAccountDefinition()
                                     .addAllAccountDefinition(accountDefinitionList)
                                     .build()
                    );
                }
        }
    }
}

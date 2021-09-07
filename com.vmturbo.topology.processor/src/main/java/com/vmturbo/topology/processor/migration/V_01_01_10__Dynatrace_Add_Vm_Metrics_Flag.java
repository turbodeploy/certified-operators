package com.vmturbo.topology.processor.migration;

import static com.vmturbo.platform.sdk.common.util.SDKProbeType.DYNATRACE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.components.common.RequiresDataInitialization.InitializationException;
import com.vmturbo.components.common.migration.AbstractMigration;
import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * This migration updates Dynatrace target info and probe info.
 * It adds a new boolean flag `doCollectVmMetrics` that is responsible for
 * enabling/disabling of collection metrics for virtual machines.
 */
public class V_01_01_10__Dynatrace_Add_Vm_Metrics_Flag extends AbstractMigration {

    private static final String PROPERTY_NAME = "collectVmMetrics";
    private static final String PROPERTY_DISPLAY_NAME = "Collect Virtual Machine Metrics";
    private static final String PROPERTY_DESCRIPTION =
            "Overwrite Hypervisor or Cloud Provider Virtual Machine metrics with data from the target";
    private static final Boolean PROPERTY_DEFAULT = Boolean.FALSE;

    private final String probeType = DYNATRACE.getProbeType();
    private final TargetStore targetStore;
    private final ProbeStore probeStore;

    /**
     * Constructor.
     *
     * @param targetStore target store.
     * @param probeStore  probe store.
     */
    public V_01_01_10__Dynatrace_Add_Vm_Metrics_Flag(TargetStore targetStore,
                                                     ProbeStore probeStore) {
        this.targetStore = targetStore;
        this.probeStore = probeStore;
    }

    @Override
    public MigrationProgressInfo doStartMigration() {
        try {
            probeStore.initialize();
            targetStore.initialize();
            updateProbeInfo();
            updateTargetInfo();
            return migrationSucceeded();
        } catch (InitializationException e) {
            final String msg = "Failed to initialize a store with error: " + e;
            return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
        } catch (ProbeException e) {
            final String msg = "Cannot update Dynatrace ProbeInfo: " + e;
            return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
        } catch (InvalidTargetException | TargetNotFoundException
               | IdentityStoreException | IdentifierConflictException e) {
            final String msg = "Cannot update Dynatrace TargetInfo: " + e;
            return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
        }
    }

    private void updateTargetInfo() throws InvalidTargetException, TargetNotFoundException,
            IdentityStoreException, IdentifierConflictException {
        final Optional<Long> probeIdOpt = probeStore.getProbeIdForType(probeType);
        if (probeIdOpt.isPresent()) {
            for (Target target : targetStore.getProbeTargets(probeIdOpt.get())) {
                if (!hasVmMetricFlag(target)) {
                    final long id = target.getId();
                    final List<AccountValue> accountValues
                            = new ArrayList<>(target.getSpec().getAccountValueList());
                    accountValues.add(createVmMetricsAccountValue());
                    final TargetSpec targetSpec = target.getSpec()
                            .toBuilder()
                            .clearAccountValue()
                            .addAllAccountValue(accountValues)
                            .build();
                    targetStore.restoreTarget(id, targetSpec);
                    targetStore.updateTarget(id, Collections.emptySet(), Optional.empty(),
                            target.getNoSecretDto().getSpec().getLastEditingUser());
                }
            }
        }
    }

    private void updateProbeInfo() throws ProbeException {
        final Optional<ProbeInfo> infoOpt = probeStore.getProbeInfoForType(probeType);
        if (infoOpt.isPresent() && !hasVmMetricFlag(infoOpt.get())) {
            probeStore.updateProbeInfo(
                    infoOpt.get()
                            .toBuilder()
                            .addAccountDefinition(createVmMetricsFlagDefEntry())
                            .build()
            );
        }
    }

    @Nonnull
    private AccountValue createVmMetricsAccountValue() {
        return TopologyProcessorDTO.AccountValue
                .newBuilder()
                .setKey(PROPERTY_NAME)
                .setStringValue(String.valueOf(PROPERTY_DEFAULT))
                .build();
    }

    @Nonnull
    private AccountDefEntry createVmMetricsFlagDefEntry() {
        return AccountDefEntry
                .newBuilder()
                .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                        .setName(PROPERTY_NAME)
                        .setPrimitiveValue(PrimitiveValue.BOOLEAN)
                        .setDisplayName(PROPERTY_DISPLAY_NAME)
                        .setDescription(PROPERTY_DESCRIPTION)
                        .setIsSecret(Boolean.FALSE)
                        .setVerificationRegex("(true|false)")
                        .build())
                .build();
    }

    private boolean hasVmMetricFlag(@Nonnull Target target) {
        return target.getSpec().getAccountValueList()
                .stream()
                .map(AccountValue::getKey)
                .anyMatch(PROPERTY_NAME::equals);
    }

    private boolean hasVmMetricFlag(@Nonnull ProbeInfo probeInfo) {
        return probeInfo.getAccountDefinitionList()
                .stream()
                .filter(AccountDefEntry::hasCustomDefinition)
                .map(AccountDefEntry::getCustomDefinition)
                .map(CustomAccountDefEntry::getName)
                .anyMatch(PROPERTY_NAME::equals);
    }
}

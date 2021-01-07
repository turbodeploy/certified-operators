package com.vmturbo.topology.processor.migration;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration;
import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.components.common.migration.AbstractMigration;
import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * This migration configures AppDynamics targets flag 'secureProxy'.
 * This flag was added to separate protocol definition between target address and target proxy.
 * Initially it is set to the value of 'secure'.
 */
public class V_01_01_03__Target_IsProxySecure_Flag extends AbstractMigration {

    private static final String SECURE = "secure";
    private static final String SECURE_PROXY = "secureProxy";
    private static final String PROXY_HOST = "proxyHost";

    private final Logger logger = LogManager.getLogger();

    protected final TargetStore targetStore;
    protected final ProbeStore probeStore;
    private final GroupScopeResolver groupScopeResolver;

    /**
     * Constructor.
     *
     * @param targetStore        - target store holding all targets.
     * @param probeStore         - remote probe store holding probe information.
     * @param groupScopeResolver - helper class for resolving groups.
     */
    @ParametersAreNonnullByDefault
    public V_01_01_03__Target_IsProxySecure_Flag(TargetStore targetStore, ProbeStore probeStore,
                                                 GroupScopeResolver groupScopeResolver) {
        this.targetStore = targetStore;
        this.probeStore = probeStore;
        this.groupScopeResolver = groupScopeResolver;
    }

    @Override
    protected MigrationProgressInfo doStartMigration() {
        try {
            probeStore.initialize();
            targetStore.initialize();
        } catch (RequiresDataInitialization.InitializationException e) {
            String msg = "Failed to initialize a store with error: " + e.getMessage();
            logger.error(msg, e);
            return updateMigrationProgress(Migration.MigrationStatus.FAILED, 0, msg);
        }
        try {
            updateProbeInfo();
        } catch (ProbeException e) {
            logger.error("Cannot update AppDynamics ProbeInfo.", e);
            return updateMigrationProgress(Migration.MigrationStatus.FAILED, 0, e.toString());
        }
        final List<Target> appDynamicsTargetsToUpdate = getAppDynamicsTargets().stream()
                .filter(this::hasProxyHost).collect(Collectors.toList());
        try {
            for (Target target : appDynamicsTargetsToUpdate) {
                updateProxyIsSecureSetting(target);
            }
        } catch (InvalidTargetException | TargetNotFoundException | IdentityStoreException | IdentifierConflictException e) {
            logger.error("Cannot update AppDynamics target.", e);
            return updateMigrationProgress(Migration.MigrationStatus.FAILED, 0, e.toString());
        }
        return migrationSucceeded();
    }

    private void updateProbeInfo() throws ProbeException {
        final Optional<ProbeInfo> infoOpt = probeStore.getProbeInfoForType(SDKProbeType.APPDYNAMICS.getProbeType());
        if (infoOpt.isPresent()) {
            // Add 'secureProxy' definition if it doesn't exist.
            if (!hasSecureProxyAccountDefinitionEntry(infoOpt.get())) {
                probeStore.updateProbeInfo(
                        infoOpt.get().toBuilder().addAccountDefinition(createProxySecureDefEntry()).build()
                );
            }
        }
    }

    protected boolean hasSecureProxyAccountDefinitionEntry(@Nonnull ProbeInfo info) {
        return findAccountDefEntry(info, SECURE_PROXY).isPresent();
    }

    protected boolean hasSecureProxyAccountValue(@Nonnull Target target) {
        return target.getSpec().getAccountValueList().stream()
                .anyMatch(val -> val.getKey().equals(SECURE_PROXY));
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    protected Optional<AccountDefEntry> findAccountDefEntry(ProbeInfo info, String entryName) {
        return info.getAccountDefinitionList().stream()
                .filter(entry -> entry.hasCustomDefinition()
                        && entry.getCustomDefinition().getName().equals(entryName))
                .findFirst();
    }

    @Nonnull
    protected AccountDefEntry createProxySecureDefEntry() {
        return AccountDefEntry
                .newBuilder()
                .setCustomDefinition(Discovery.CustomAccountDefEntry.newBuilder()
                        .setName(SECURE_PROXY)
                        .setPrimitiveValue(PrimitiveValue.BOOLEAN)
                        .setDisplayName("Secure Proxy Connection")
                        .setDescription("Use SSL to connect to the proxy host")
                        .setIsSecret(Boolean.FALSE)
                        .setVerificationRegex("(true|false)")
                        .build())
                .build();
    }

    @Nonnull
    private List<Target> getAppDynamicsTargets() {
        return getTargetsByType(SDKProbeType.APPDYNAMICS);
    }

    @Nonnull
    protected List<Target> getTargetsByType(@Nonnull SDKProbeType probeType) {
        final Optional<Long> probeIdOpt = probeStore.getProbeIdForType(probeType.getProbeType());
        if (probeIdOpt.isPresent()) {
            final Long appDynamicsProbeId = probeIdOpt.get();
            return targetStore.getProbeTargets(appDynamicsProbeId);
        }
        return Collections.emptyList();
    }

    private boolean hasProxyHost(@Nonnull Target target) {
        final List<AccountValue> accountVals = target.getMediationAccountVals(groupScopeResolver);
        return accountVals.stream()
                .anyMatch(accValue -> accValue.getKey().equals(PROXY_HOST)
                        && StringUtils.isNotEmpty(accValue.getStringValue()));
    }

    private void updateProxyIsSecureSetting(@Nonnull Target target) throws InvalidTargetException,
            TargetNotFoundException, IdentityStoreException, IdentifierConflictException {
        final Optional<AccountValue> secureOpt = target.getMediationAccountVals(groupScopeResolver).stream()
                .filter(accountValue -> accountValue.getKey().equals(SECURE))
                .findFirst();
        if (secureOpt.isPresent()) {
            final AccountValue secure = secureOpt.get();
            final TopologyProcessorDTO.AccountValue secureProxy =
                    createSecureProxyAccountValue(secure.getStringValue());
            targetStore.updateTarget(target.getId(), Collections.singletonList(secureProxy));
        }
    }

    @Nonnull
    protected TopologyProcessorDTO.AccountValue createSecureProxyAccountValue(@Nonnull String val) {
        return TopologyProcessorDTO.AccountValue
                .newBuilder()
                .setKey(SECURE_PROXY)
                .setStringValue(val)
                .build();
    }
}

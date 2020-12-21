package com.vmturbo.topology.processor.migration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration;
import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * This migration updates all targets which accounts classes have been changed with new common proxy
 * settings. Some fields in a target have not common name, like 'proxy' should be 'proxyHost' now.
 * What should be renamed is defined in the 'renameConfiguration' map.
 */
public class V_01_01_05__Target_Common_Proxy_Settings extends V_01_01_03__Target_IsProxySecure_Flag {

    private static final Logger LOGGER = LogManager.getLogger();

    static final Pair<String, String> HOST_RENAME = new Pair<>("proxy", "proxyHost");
    static final Pair<String, String> PORT_RENAME = new Pair<>("port", "proxyPort");
    static final Pair<String, String> USER_RENAME = new Pair<>("proxyUser", "proxyUsername");
    private final Map<SDKProbeType, List<Pair<String, String>>> renameConfiguration;

    /**
     * Constructor.
     *
     * @param targetStore        - target store holding all targets.
     * @param probeStore         - remote probe store holding probe information.
     * @param groupScopeResolver - helper class for resolving group
     */
    @ParametersAreNonnullByDefault
    public V_01_01_05__Target_Common_Proxy_Settings(TargetStore targetStore, ProbeStore probeStore,
                                                    GroupScopeResolver groupScopeResolver) {
        super(targetStore, probeStore, groupScopeResolver);
        this.renameConfiguration = createRenameConfig();
    }

    @Override
    protected MigrationProgressInfo doStartMigration() {
        try {
            probeStore.initialize();
            targetStore.initialize();
        } catch (RequiresDataInitialization.InitializationException e) {
            String msg = "Failed to initialize a store with error: " + e.getMessage();
            LOGGER.error(msg, e);
            return updateMigrationProgress(Migration.MigrationStatus.FAILED, 0, msg);
        }
        try {
            updateProbeInfo();
        } catch (ProbeException e) {
            LOGGER.error("Cannot update ProbeInfo.", e);
            return updateMigrationProgress(Migration.MigrationStatus.FAILED, 0, e.toString());
        }
        updateTargetData();
        return migrationSucceeded();
    }

    private void updateTargetData() {
        for (SDKProbeType probeType : renameConfiguration.keySet()) {
            for (Target target : getTargetsByType(probeType)) {
                final List<AccountValue> accountVals = new ArrayList<>(target.getSpec().getAccountValueList());
                // Add 'secureProxy'
                if (!hasSecureProxyAccountValue(target)) {
                    accountVals.add(createSecureProxyAccountValue("false"));
                }
                // Rename target's fields
                if (hasRenamingFields(accountVals, probeType)) {
                    for (Pair<String, String> renamePair : renameConfiguration.get(probeType)) {
                        final String oldValue = renamePair.getFirst();
                        final String newValue = renamePair.getSecond();
                        final AccountValue accountValueOld = accountVals.stream()
                                .filter(val -> val.getKey().equals(oldValue))
                                .findFirst().orElse(null);
                        if (accountValueOld != null) {
                            final AccountValue accountValueNew = accountValueOld.toBuilder().setKey(newValue).build();
                            accountVals.set(accountVals.indexOf(accountValueOld), accountValueNew);
                        }
                    }
                }
                try {
                    targetStore.restoreTarget(target.getId(), target.getSpec().toBuilder()
                            .clearAccountValue().addAllAccountValue(accountVals).build());
                    targetStore.updateTarget(target.getId(), Collections.emptySet(), Optional.empty());
                    LOGGER.info("Target migrated: {} ({})", target.getId(), probeType.getProbeType());
                } catch (InvalidTargetException | TargetNotFoundException | IdentityStoreException | IdentifierConflictException e) {
                    LOGGER.error("Cannot update target values.", e);
                }
            }
        }
    }

    @ParametersAreNonnullByDefault
    private boolean hasRenamingFields(List<AccountValue> accountVals, SDKProbeType probeType) {
        final Set<String> accountValsNames = accountVals.stream()
                .map(AccountValue::getKey).collect(Collectors.toSet());
        final Set<String> renameValsNames = renameConfiguration.get(probeType).stream()
                .map(Pair::getFirst).collect(Collectors.toSet());
        return !Sets.intersection(accountValsNames, renameValsNames).isEmpty();
    }

    private void updateProbeInfo() throws ProbeException {
        for (SDKProbeType probeType : renameConfiguration.keySet()) {
            final Optional<ProbeInfo> infoOpt = probeStore.getProbeInfoForType(probeType.getProbeType());
            if (infoOpt.isPresent()) {
                ProbeInfo info = infoOpt.get();
                // Rename fields
                for (Pair<String, String> renamePair : renameConfiguration.get(probeType)) {
                    final String oldValue = renamePair.getFirst();
                    final String newValue = renamePair.getSecond();
                    final Optional<AccountDefEntry> optValue = findAccountDefEntry(info, oldValue);
                    if (optValue.isPresent()) {
                        final AccountDefEntry adeValue = optValue.get();
                        final AccountDefEntry adeValueNew = adeValue.toBuilder()
                                .setCustomDefinition(adeValue
                                        .getCustomDefinition()
                                        .toBuilder()
                                        .setName(newValue)
                                        .build())
                                .build();
                        final int index = info.getAccountDefinitionList().indexOf(adeValue);
                        if (index != -1) {
                            info = info.toBuilder()
                                    .removeAccountDefinition(index)
                                    .addAccountDefinition(adeValueNew)
                                    .build();
                        }
                    }
                }
                // Add 'secureProxy'
                if (!hasSecureProxyAccountDefinitionEntry(info)) {
                    info = info.toBuilder().addAccountDefinition(createProxySecureDefEntry()).build();
                }
                probeStore.updateProbeInfo(info);
            }
        }
    }

    /**
     * Rename configuration.
     * --------------------
     * TYPE                     CLASS (AccountDefinition)                                 FIELDS TO UPDATE          FIELDS TO KEEP
     * ----                     -------------------------                                 ----------------          --------------
     * AWS                      com.vmturbo.mediation.aws.client.AwsAccountBase           proxy,port,proxyUser
     * AWS Billing              com.vmturbo.mediation.aws.billing.AwsBillingAccount       proxy,port,proxyUser
     * AWS Cost                 com.vmturbo.mediation.aws.cost.AwsCostAccount             proxy,port,proxyUser
     * Azure Subscription       com.vmturbo.mediation.azure.AzureAccount                  proxy,port,proxyUser
     * Azure Cost               com.vmturbo.mediation.azure.cost.AzureCostAccount         proxy,port,proxyUser
     * Azure EA                 com.vmturbo.mediation.azure.ea.AzureEAAccount             proxyUser                  proxyHost,proxyPort
     * Azure Service Principal  com.vmturbo.mediation.azure.sp.AzureSPAccount             proxyUser                  proxyHost,proxyPort
     * GCP                      com.vmturbo.mediation.gcp.client.GcpAccount               proxy,port,proxyUser
     * GCP Cost                 com.vmturbo.mediation.gcp.cost.GcpCostAccount             proxy,port,proxyUser
     * Pivotal Ops Manager      com.vmturbo.mediation.pivotal.opsman.PivotalOpsmanAccount proxy                      proxyPort
     * ServiceNow               com.vmturbo.mediation.servicenow.ServiceNowAccount        proxyUser                  proxyHost,proxyPort
     * AppInsights              com.vmturbo.mediation.appinsights.AppInsightsAccount      proxy,port,proxyUser
     *
     * @return configuration map.
     */
    @Nonnull
    protected Map<SDKProbeType, List<Pair<String, String>>> createRenameConfig() {
        final Map<SDKProbeType, List<Pair<String, String>>> map = Maps.newHashMap();
        map.put(SDKProbeType.APPINSIGHTS,
                Arrays.asList(HOST_RENAME, PORT_RENAME, USER_RENAME));
        map.put(SDKProbeType.AWS,
                Arrays.asList(HOST_RENAME, PORT_RENAME, USER_RENAME));
        map.put(SDKProbeType.AWS_BILLING,
                Arrays.asList(HOST_RENAME, PORT_RENAME, USER_RENAME));
        map.put(SDKProbeType.AWS_COST,
                Arrays.asList(HOST_RENAME, PORT_RENAME, USER_RENAME));
        map.put(SDKProbeType.AZURE,
                Arrays.asList(HOST_RENAME, PORT_RENAME, USER_RENAME));
        map.put(SDKProbeType.AZURE_COST,
                Arrays.asList(HOST_RENAME, PORT_RENAME, USER_RENAME));
        map.put(SDKProbeType.AZURE_EA,
                Arrays.asList(USER_RENAME));
        map.put(SDKProbeType.AZURE_SERVICE_PRINCIPAL,
                Arrays.asList(USER_RENAME));
        map.put(SDKProbeType.GCP,
                Arrays.asList(HOST_RENAME, PORT_RENAME, USER_RENAME));
        map.put(SDKProbeType.GCP_COST,
                Arrays.asList(HOST_RENAME, PORT_RENAME, USER_RENAME));
        map.put(SDKProbeType.PIVOTAL_OPSMAN,
                Arrays.asList(HOST_RENAME));
        map.put(SDKProbeType.SERVICENOW,
                Arrays.asList(USER_RENAME));
        return map;
    }
}

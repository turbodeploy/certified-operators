package com.vmturbo.integrations.intersight.targetsync;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.cisco.intersight.client.model.AssetScopedTargetConnection;
import com.cisco.intersight.client.model.AssetTarget;
import com.cisco.intersight.client.model.AssetTarget.TargetTypeEnum;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.connector.intersight.IntersightConnection;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.AccountFieldValueType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.dto.TargetInputFields;

/**
 * This is a factory consisting of a set of static methods to assist target conversion from
 * Intersight {@link AssetTarget} to topology processor target data structure.
 */
public class IntersightTargetConverter {
    private static final Logger logger = LogManager.getLogger();

    protected static final String INTERSIGHT_ADDRESS = "address";
    protected static final String INTERSIGHT_PORT = "port";
    protected static final String INTERSIGHT_CLIENTID = "clientId";
    protected static final String INTERSIGHT_CLIENTSECRET = "clientSecret";

    /**
     * The name of the account input field corresponding to the target scope.
     */
    protected static final String TARGET_SCOPE_FIELD_NAME = "targetEntities";

    private IntersightTargetConverter() {}

    /**
     * Return the scope id extracted from any {@link AssetScopedTargetConnection} in the input
     * {@link AssetTarget}.
     *
     * @param assetTarget the target in Intersight {@link AssetTarget} data structure
     * @return the scope id if any in the form of {@link Optional}
     */
    protected static Optional<String> getScopeId(@Nonnull AssetTarget assetTarget) {
        return Optional.ofNullable(assetTarget.getConnections())
                .orElse(Collections.emptyList()).stream()
                .filter(AssetScopedTargetConnection.class::isInstance)
                .map(AssetScopedTargetConnection.class::cast)
                .map(AssetScopedTargetConnection::getScope)
                .findAny();
    }

    /**
     * Construct a {@link TargetInputFields} corresponding to an Intersight {@link AssetTarget} for
     * use to call topology processor to add or update targets.
     *
     * @param assetTarget the target in Intersight {@link AssetTarget} data structure
     * @param assistId the assist device MOID in {@link Optional} form or empty if no assist
     *                 associated with this target
     * @param probeInfo the probe info in topology processor API data structure
     * @return the resulting {@link TargetInputFields}
     */
    @Nullable
    protected static TargetInputFields inputFields(@Nonnull final AssetTarget assetTarget,
            @Nonnull Optional<String> assistId, @Nonnull final ProbeInfo probeInfo) {
        Objects.requireNonNull(assetTarget);
        Objects.requireNonNull(probeInfo);
        Objects.requireNonNull(assistId);
        final List<InputField> inputFields = probeInfo.getAccountDefinitions().stream()
                .map(accountDefEntry -> {
                    final String name = accountDefEntry.getName();
                    final String value;
                    // replace all string id fields with the target moid, assuming there
                    // is at least one such field; for all other fields, try to fill in
                    // something legal
                    if (probeInfo.getIdentifyingFields().contains(name)
                            && accountDefEntry.getValueType() == AccountFieldValueType.STRING) {
                        value = getConvertedTargetMoId(assetTarget);
                        if (value == null) {
                            return null;
                        }
                    } else if (TARGET_SCOPE_FIELD_NAME.equals(name)) {
                        value = getScopeId(assetTarget).orElse("");
                    } else if (accountDefEntry.getDefaultValue() != null) {
                        value = accountDefEntry.getDefaultValue();
                    } else if (accountDefEntry.getAllowedValues() != null
                            && accountDefEntry.getAllowedValues().size() > 0) {
                        value = accountDefEntry.getAllowedValues().get(0);
                    } else {
                        switch (accountDefEntry.getValueType()) {
                            case BOOLEAN:
                                value = "false";
                                break;
                            case NUMERIC:
                                value = "0";
                                break;
                            default:
                                value = "";
                                break;
                        }
                    }
                    return new InputField(name, Objects.toString(value, ""), Optional.empty());
                })
                .collect(Collectors.toList());
        return new TargetInputFields(inputFields, assistId);
    }

    /**
     * Construct a {@link TargetInputFields} corresponding to the Intersight target for use to
     * call topology processor to add.
     *
     * @param intersightConnection the Intersight connection info
     * @param intersightProbeInfo the Intersight probe info
     * @return the resulting {@link TargetInputFields}
     */
    protected static TargetInputFields inputFields(
            @Nonnull final IntersightConnection intersightConnection,
            @Nonnull final ProbeInfo intersightProbeInfo) {
        Objects.requireNonNull(intersightConnection);
        Objects.requireNonNull(intersightProbeInfo);
        final List<InputField> inputFields = intersightProbeInfo.getAccountDefinitions().stream()
                .map(accountDefEntry -> {
            final String name = accountDefEntry.getName();
            final String strValue;
            switch (name) {
                case INTERSIGHT_CLIENTID:
                    strValue = intersightConnection.getClientId();
                    break;
                case INTERSIGHT_CLIENTSECRET:
                    strValue = intersightConnection.getClientSecret();
                    break;
                case INTERSIGHT_ADDRESS:
                    strValue = intersightConnection.getAddress();
                    break;
                case INTERSIGHT_PORT:
                    strValue = intersightConnection.getPort().toString();
                    break;
                default:
                    switch (accountDefEntry.getValueType()) {
                        case NUMERIC:
                            strValue = "0";
                            break;
                        case STRING:
                        default:
                            strValue = "";
                            break;
                    }
                    break;
            }
            return new InputField(name, Objects.toString(strValue, ""), Optional.empty());
        }).collect(Collectors.toList());

        return new TargetInputFields(inputFields, Optional.empty());
    }

    /**
     * Find the corresponding set of {@link SDKProbeType}s for the given {@link AssetTarget} from
     * Intersight.  Maybe should make this a config map?
     *
     * @param assetTarget the {@link AssetTarget} from Intersight
     * @return a set of {@link SDKProbeType}s corresponding to the target
     */
    @Nonnull
    protected static Collection<SDKProbeType> findProbeType(@Nonnull final AssetTarget assetTarget) {
        final TargetTypeEnum targetType = Objects.requireNonNull(assetTarget).getTargetType();
        if (targetType == null) {
            logger.debug("Null Intersight target type in asset.Target {}", assetTarget.getMoid());
            return Collections.emptySet();
        }
        switch (targetType) {
            case VMWAREVCENTER:
                return Collections.singleton(SDKProbeType.VCENTER);
            case APPDYNAMICS:
                return Collections.singleton(SDKProbeType.APPDYNAMICS);
            case PURESTORAGEFLASHARRAY:
                return Collections.singleton(SDKProbeType.PURE);
            case NETAPPONTAP:
                return Collections.singleton(SDKProbeType.NETAPP);
            case EMCSCALEIO:
                return Collections.singleton(SDKProbeType.SCALEIO);
            case EMCVMAX:
                return Collections.singleton(SDKProbeType.VMAX);
            case EMCVPLEX:
                return Collections.singleton(SDKProbeType.VPLEX);
            case EMCXTREMIO:
                return Collections.singleton(SDKProbeType.XTREMIO);
            case DELLCOMPELLENT:
                return Collections.singleton(SDKProbeType.COMPELLENT);
            case HPE3PAR:
                return Collections.singleton(SDKProbeType.HPE_3PAR);
            case HPEONEVIEW:
                return Collections.singleton(SDKProbeType.ONEVIEW);
            case NUTANIXACROPOLIS:
                return Collections.singleton(SDKProbeType.NUTANIX);
            case REDHATENTERPRISEVIRTUALIZATION:
                return Collections.singleton(SDKProbeType.RHV);
            case MICROSOFTSQLSERVER:
                return Collections.singleton(SDKProbeType.MSSQL);
            case MICROSOFTAZUREENTERPRISEAGREEMENT:
                return Collections.singleton(SDKProbeType.AZURE_EA);
            case MICROSOFTAZURESERVICEPRINCIPAL:
                return Collections.singleton(SDKProbeType.AZURE_SERVICE_PRINCIPAL);
            case MICROSOFTHYPERV:
                return Collections.singleton(SDKProbeType.HYPERV);
            case DYNATRACE:
                return Collections.singleton(SDKProbeType.DYNATRACE);
            case AMAZONWEBSERVICE:
                return Collections.singleton(SDKProbeType.AWS);
            case AMAZONWEBSERVICEBILLING:
                return Collections.singleton(SDKProbeType.AWS_BILLING);
            case MICROSOFTAZUREAPPLICATIONINSIGHTS:
                return Collections.singleton(SDKProbeType.APPINSIGHTS);
            case CLOUDFOUNDRY:
                return Collections.singleton(SDKProbeType.CLOUD_FOUNDRY);
            case KUBERNETES:
                return Collections.singleton(SDKProbeType.KUBERNETES);
            case NEWRELIC:
                return Collections.singleton(SDKProbeType.NEWRELIC);
            case MYSQLSERVER:
                return Collections.singleton(SDKProbeType.MYSQL);
            case VMWAREHORIZON:
                return Collections.singleton(SDKProbeType.VMWARE_HORIZON_VIEW);
            case MICROSOFTSYSTEMCENTERVIRTUALMACHINEMANAGER:
                return Collections.singleton(SDKProbeType.VMM);
            case DATADOG:
                return Collections.singleton(SDKProbeType.DATADOG);
            case SERVICENOW:
                return Collections.singleton(SDKProbeType.SERVICENOW);
            default:
                logger.debug("Unsupported Intersight target type {} in asset.Target {}",
                        assetTarget.getTargetType(), assetTarget.getMoid());
                return Collections.emptySet();
        }
    }

    /**
     * Get the converted MO ID of the given {@link AssetTarget} from Intersight.
     * For NewRelic target hex target MO id will be converted to the string representation of its
     * equivalent decimal integer. Null be be returned if Newrelic target MO id conversion fails.
     *
     * @param assetTarget the {@link AssetTarget} from Intersight
     * @return target MO id of the Intersight target.
     */
    @Nullable
    protected static String getConvertedTargetMoId(@Nonnull final AssetTarget assetTarget) {
        final TargetTypeEnum targetType = assetTarget.getTargetType();
        String moid = assetTarget.getMoid();
        if (targetType != TargetTypeEnum.NEWRELIC) {
            return moid;
        }
        // NewRelic target needs the account Id to be integer.
        // We need to convert hexadecimal target MO id to literal of its equivalent decimal integer.
        try {
            final BigInteger valBigInt = new BigInteger(moid, 16);
            return valBigInt.toString();
        } catch (NumberFormatException e) {
            logger.error("Error converting {} MO Id {} to BigInteger.", targetType, moid);
            return null;
        }
    }
}

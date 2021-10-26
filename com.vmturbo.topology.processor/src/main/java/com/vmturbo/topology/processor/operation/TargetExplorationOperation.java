package com.vmturbo.topology.processor.operation;

import static com.vmturbo.platform.sdk.common.util.SDKUtil.ERROR_TYPE_ERROR_TYPE_INFO_MAP;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * A superclass for Validation and Discovery operations to store info about error type(s) and various error details.
 */
public abstract class TargetExplorationOperation extends Operation {
    private final Collection<ErrorTypeInfo> errorTypeInfos = new HashSet<>(1);
    /**
     * The list of reports for discovery/validation stages.
     */
    private final List<ProbeStageDetails> stagesReports = new ArrayList<>();

    /**
     * Constructor, passes parameters from children to superior class.
     *
     * @param probeId               ID of the probe
     * @param targetId              ID of the target
     * @param identityProvider      identity provider
     * @param durationMetricSummary duration metric summary
     */
    public TargetExplorationOperation(long probeId, long targetId,
                                      IdentityProvider identityProvider, DataMetricSummary durationMetricSummary) {
        super(probeId, targetId, identityProvider, durationMetricSummary);
    }

    @Override
    @Nonnull
    public Operation addErrors(@Nonnull final List<ErrorDTO> errors) {
        super.addErrors(errors);
        errors.forEach(errorDTO -> {
            convertLegacyErrorType(errorDTO).ifPresent(errorTypeInfos::add);
            Optional.of(errorDTO.getErrorTypeInfoList()).ifPresent(errorTypeInfos::addAll);
        });
        return this;
    }

    @Override
    @Nonnull
    public Operation addError(@Nonnull final ErrorDTO error) {
        super.addError(error);
        convertLegacyErrorType(error).ifPresent(errorTypeInfos::add);
        errorTypeInfos.addAll(error.getErrorTypeInfoList());
        return this;
    }

    /**
     * Gets error type(s) for errors happened during Validation or Discovery.
     *
     * @return a list of {@link ErrorTypeInfo} items. Empty if no errors registered.
     */
    @Nonnull
    public Collection<ErrorTypeInfo> getErrorTypeInfos() {
        return Collections.unmodifiableCollection(errorTypeInfos);
    }

    /**
     * Adds stages reports of the probe operation.
     *
     * @param operationStagesReports stages reports
     */
    public void addStagesReports(@Nonnull Collection<ProbeStageDetails> operationStagesReports) {
        stagesReports.addAll(operationStagesReports);
    }

    /**
     * Gets the list of stage reports of the probe operation.
     *
     * @return stages reports
     */
    @Nonnull
    public List<ProbeStageDetails> getStagesReports() {
        return stagesReports;
    }

    private static Optional<ErrorTypeInfo> convertLegacyErrorType(@Nullable final ErrorDTO error) {
        if (error.hasErrorType()) {
            return Optional.of(ERROR_TYPE_ERROR_TYPE_INFO_MAP.get(error.getErrorType()));
        } else {
            return Optional.empty();
        }
    }
}

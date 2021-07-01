package com.vmturbo.topology.processor.operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ProbeStageDetails;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * A superclass for Validation and Discovery operations to store info about error type(s).
 */
public abstract class TargetExplorationOperation extends Operation {
    private final List<ErrorDTO.ErrorType> errorTypes = new ArrayList<>(1);
    /**
     * The list of reports for discovery/validation stages.
     */
    private final List<ProbeStageDetails> stagesReports = new ArrayList<>();

    /**
     * Constructor, passes parameters from children to superior class.
     * @param probeId ID of the probe
     * @param targetId ID of the target
     * @param identityProvider identity provider
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
        errorTypes.addAll(errors.stream()
                        .map(ErrorDTO::getErrorType)
                        .collect(Collectors.toList()));
        return this;
    }

    @Override
    @Nonnull
    public Operation addError(@Nonnull final ErrorDTO error) {
        super.addError(error);
        errorTypes.add(error.getErrorType());
        return this;
    }

    /**
     * Gets error type(s) for errors happened during Validation or Discovery.
     * @return a list of {@link ErrorDTO.ErrorType} items. Empty if no errors registered.
     */
    @Nonnull
    public List<ErrorDTO.ErrorType> getErrorTypes() {
        return Collections.unmodifiableList(errorTypes);
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
}

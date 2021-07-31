package com.vmturbo.topology.processor.planexport;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;

/**
 * An interface for actionDTO converting to executionDTO. Different actions can have classes
 * implementing the convert method respectively.
 */
public interface ExecutionDTOConverter {

    /**
     * The method to convert an {@link Action} to an {@link ActionExecutionDTO}.
     *
     * @param action the given action DTO.
     * @return an {@link ActionExecutionDTO}.
     * @throws ActionDTOConversionException the exception encountered in conversion
     */
    @Nonnull
    ActionExecutionDTO convert(@Nonnull Action action) throws ActionDTOConversionException;
}

package com.vmturbo.topology.processor.operation;

import static org.junit.Assert.*;

import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;

/**
 * Operation unit tests.
 */
public class OperationTest {

    @Test
    public void testHumanReadableErrorNoEntity() {
        ErrorDTO error = ErrorDTO.newBuilder()
            .setSeverity(ErrorSeverity.CRITICAL)
            .setDescription("Unexpected vampire squid attack")
            .build();
        assertEquals("CRITICAL: Unexpected vampire squid attack", Operation.humanReadableError(error));
    }

    @Test
    public void testHumanReadableErrorWithEntity() {
        ErrorDTO error = ErrorDTO.newBuilder()
            .setSeverity(ErrorSeverity.WARNING)
            .setDescription("Boozlers set to value 3")
            .setEntityType(EntityType.VIRTUAL_MACHINE.name())
            .setEntityUuid("Uia-w4")
            .build();

        assertEquals(
            "WARNING: Boozlers set to value 3 (VIRTUAL_MACHINE Uia-w4)",
            Operation.humanReadableError(error)
        );
    }

}
package com.vmturbo.reserved.instance.coverage.allocator.topology;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * An implementation of {@link CoverageEntityInfo} for virtual machines.
 */
@HiddenImmutableImplementation
@Immutable
public interface VirtualMachineInfo extends CoverageEntityInfo {

    /**
     * The platform of the VM.
     * @return The platform of the VM.
     */
    @Nonnull
    OSType platform();

    /**
     * The tenancy of the VM.
     * @return The tenancy of the VM.
     */
    @Nonnull
    Tenancy tenancy();

    /**
     * The entity type (will always be {@link EntityType#VIRTUAL_MACHINE_VALUE}).
     * @return The entity type (will always be {@link EntityType#VIRTUAL_MACHINE_VALUE}).
     */
    @Derived
    default int entityType() {
        return EntityType.VIRTUAL_MACHINE_VALUE;
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link VirtualMachineInfo} instances.
     */
    class Builder extends ImmutableVirtualMachineInfo.Builder {}
}

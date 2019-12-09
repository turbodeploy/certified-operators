package com.vmturbo.cost.calculation;

import static org.mockito.Mockito.when;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeTierConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.DatabaseConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.NetworkConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.VirtualVolumeConfig;

/**
 * The entity class to use as the template parameter in cost calculation tests.
 */
public final class TestEntityClass {

    private final long id;

    private final int type;

    private final Optional<ComputeConfig> computeConfig;

    private TestEntityClass(final long id,
                            final int type,
                            final Optional<ComputeConfig> computeConfig) {
        this.id = id;
        this.type = type;
        this.computeConfig = computeConfig;
    }

    /**
     * Create a new builder.
     *
     * @param id The ID.
     * @return The {@link Builder}.
     */
    public static Builder newBuilder(final long id) {
        return new Builder().setId(id);
    }

    public long getId() {
        return id;
    }

    public int getType() {
        return type;
    }

    public Optional<ComputeConfig> getComputeConfig() {
        return computeConfig;
    }

    public static class Builder {
        private long id = -1;

        private int type = -1;

        private EntityState state = EntityState.POWERED_ON;

        private Optional<ComputeConfig> computeConfig = Optional.empty();

        private Optional<DatabaseConfig> databaseConfig = Optional.empty();

        private Optional<NetworkConfig> networkConfig = Optional.empty();

        private Optional<VirtualVolumeConfig> volumeConfig = Optional.empty();

        private Optional<ComputeTierConfig> computeTierConfig = Optional.empty();

        @Nonnull
        public Builder setId(final long id) {
            this.id = id;
            return this;
        }

        @Nonnull
        public Builder setType(final int type) {
            this.type = type;
            return this;
        }

        @Nonnull
        public Builder setEntityState(final EntityState state) {
            this.state = state;
            return this;
        }

        @Nonnull
        public Builder setComputeConfig(@Nonnull final ComputeConfig computeConfig) {
            this.computeConfig = Optional.of(computeConfig);
            return this;
        }

        @Nonnull
        public Builder setDatabaseConfig(@Nonnull final DatabaseConfig databaseConfig) {
            this.databaseConfig = Optional.of(databaseConfig);
            return this;
        }

        @Nonnull
        public Builder setNetworkConfig(@Nonnull final NetworkConfig ipConfig) {
            this.networkConfig = Optional.of(ipConfig);
            return this;
        }

        @Nonnull
        public Builder setVolumeConfig(@Nonnull final VirtualVolumeConfig volumeConfig) {
            this.volumeConfig = Optional.of(volumeConfig);
            return this;
        }

        @Nonnull
        public Builder setComputeTierConfig(@Nonnull final ComputeTierConfig computeTierConfig) {
            this.computeTierConfig = Optional.of(computeTierConfig);
            return this;
        }

        @Nonnull
        public TestEntityClass build(@Nonnull final EntityInfoExtractor<TestEntityClass> infoExtractor) {
            final TestEntityClass ret = new TestEntityClass(id, type, computeConfig);
            when(infoExtractor.getId(ret)).thenReturn(id);
            when(infoExtractor.getName(ret)).thenReturn(Long.toString(id));
            when(infoExtractor.getEntityState(ret)).thenReturn(state);
            when(infoExtractor.getVolumeConfig(ret)).thenReturn(volumeConfig);
            when(infoExtractor.getComputeConfig(ret)).thenReturn(computeConfig);
            when(infoExtractor.getComputeTierConfig(ret)).thenReturn(computeTierConfig);
            when(infoExtractor.getEntityType(ret)).thenReturn(type);
            when(infoExtractor.getDatabaseConfig(ret)).thenReturn(databaseConfig);
            when(infoExtractor.getNetworkConfig(ret)).thenReturn(networkConfig);
            return ret;
        }
    }
}

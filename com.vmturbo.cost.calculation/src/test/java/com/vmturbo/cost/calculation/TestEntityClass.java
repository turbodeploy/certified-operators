package com.vmturbo.cost.calculation;

import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.DatabaseConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.NetworkConfig;

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

        private Optional<ComputeConfig> computeConfig = Optional.empty();

        private Optional<DatabaseConfig> databaseConfig = Optional.empty();

        private Optional<NetworkConfig> networkConfig = Optional.empty();

        private List<BiConsumer<TestEntityClass, EntityInfoExtractor<TestEntityClass>>> extractorConsumers = new ArrayList<>();

        @Nonnull
        public Builder setId(final long id) {
            this.id = id;
            extractorConsumers.add((entity, infoExtractor) -> when(infoExtractor.getId(entity)).thenReturn(id));
            return this;
        }

        @Nonnull
        public Builder setType(final int type) {
            this.type = type;
            extractorConsumers.add((entity, infoExtractor) -> when(infoExtractor.getEntityType(entity)).thenReturn(type));
            return this;
        }

        @Nonnull
        public Builder setComputeConfig(@Nonnull final ComputeConfig computeConfig) {
            this.computeConfig = Optional.of(computeConfig);
            extractorConsumers.add((entity, infoExtractor) -> when(infoExtractor.getComputeConfig(entity)).thenReturn(Optional.of(computeConfig)));
            return this;
        }

        @Nonnull
        public Builder setDatabaseConfig(@Nonnull final DatabaseConfig databaseConfig) {
            this.databaseConfig = Optional.of(databaseConfig);
            extractorConsumers.add((entity, infoExtractor) -> when(infoExtractor.getDatabaseConfig(entity)).thenReturn(Optional.of(databaseConfig)));
            return this;
        }

        @Nonnull
        public Builder setNetworkConfig(@Nonnull final NetworkConfig ipConfig) {
            this.networkConfig = Optional.of(ipConfig);
            extractorConsumers.add((entity, infoExtractor) -> when(infoExtractor.getNetworkConfig(entity)).thenReturn(Optional.of(ipConfig)));
            return this;
        }

        @Nonnull
        public TestEntityClass build(@Nonnull final EntityInfoExtractor<TestEntityClass> infoExtractor) {
            final TestEntityClass ret = new TestEntityClass(id, type, computeConfig);
            extractorConsumers.forEach(consumer -> consumer.accept(ret, infoExtractor));
            return ret;
        }
    }
}

package com.vmturbo.mediation.udt.explore.collectors;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.vmturbo.mediation.udt.explore.DataProvider;
import com.vmturbo.mediation.udt.inventory.UdtEntity;

/**
 * Abstract collector for populating UDT entities.
 *
 * @param <T> - type of topology data definition (manual/automated).
 */
public abstract class UdtCollector<T> {

    protected final Long definitionId;
    protected final T definition;

    /**
     * Constructor.
     *
     * @param definitionId - ID of topology data definition.
     * @param definition   - topology data definition instance.
     */
    @ParametersAreNonnullByDefault
    protected UdtCollector(Long definitionId, T definition) {
        this.definitionId = definitionId;
        this.definition = definition;
    }

    /**
     * A method for populating UDT entities.
     *
     * @param dataProvider - an instance of {@link DataProvider}.
     * @return a set of UDT entities.
     */
    @Nonnull
    public abstract Set<UdtEntity> collectEntities(@Nonnull DataProvider dataProvider);
}

package com.vmturbo.group.topologydatadefinition;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionEntry;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.IdentityStoreUpdate;

/**
 * Store for TopologyDataDefinitions. Will eventually interact with DB.
 */
public class TopologyDataDefinitionStore implements DiagsRestorable {

    /**
     * The file name for the TopolodyDataDefinition dump collected from the
     * {@link TopologyDataDefinitionStore}.
     */
    private static final String TOPOLOGY_DATA_DEFINITION_DUMP_FILE = "topology_data_definition_dump";

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;

    private final IdentityStore<TopologyDataDefinition> identityStore;

    // TODO remove this map once we are actually writing to/reading from a DB table.
    private final Map<Long, TopologyDataDefinition> oidToTopologyDataDefinition;

    /**
     * Create a TopologyDataDefinitionStore.
     *
     * @param dslContext for interacting with DB.
     * @param identityStore to create OIDs and get existing OIDs of TopologyDataDefinitions.
     */
    public TopologyDataDefinitionStore(@Nonnull DSLContext dslContext,
                                       @Nonnull IdentityStore<TopologyDataDefinition> identityStore) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.identityStore = Objects.requireNonNull(identityStore);
        this.oidToTopologyDataDefinition = Maps.newHashMap();
    }

    /**
     * Create a new TopologyDataDefinition and persist it.
     *
     * @param definition the {@link TopologyDataDefinition} to create.
     * @return {@link TopologyDataDefinitionEntry} that includes the OID and the
     * TopologyDataDefinition.
     * @throws DuplicateNameException if there is already an existing {@link TopologyDataDefinition}
     * with the same matching attributes as this one.
     * @throws IdentityStoreException if there is a problem fetching an OID for this
     * {@link TopologyDataDefinition}.
     */
    public TopologyDataDefinitionEntry createTopologyDataDefinition(@Nonnull TopologyDataDefinition definition)
        throws DuplicateNameException, IdentityStoreException {
        IdentityStoreUpdate<TopologyDataDefinition> update = identityStore.fetchOrAssignItemOids(
            Collections.singletonList(Objects.requireNonNull(definition)));
        final long oid = update.getOldItems().isEmpty() ? update.getNewItems().get(definition)
            : update.getOldItems().get(definition);
        if (oidToTopologyDataDefinition.containsKey(oid)) {
            throw new DuplicateNameException("TopologyDataDefinition already exists with OID: "
                + update.getOldItems().get(definition));
        } else {
            logger.debug("Creating new TopologyDataDefinition {} with OID {}.",
                () -> definition, () -> oid);
            oidToTopologyDataDefinition.put(oid, definition);
            return TopologyDataDefinitionEntry.newBuilder()
                .setId(update.getNewItems().get(definition))
                .setDefinition(definition)
                .build();
        }
    }

    /**
     * Get the {@link TopologyDataDefinition} with the given OID.
     *
     * @param id the OID of the {@link TopologyDataDefinition}.
     * @return Optional of {@link TopologyDataDefinition} if it exist or Optional.empty otherwise.
     */
    public Optional<TopologyDataDefinition> getTopologyDataDefinition(long id) {
        return Optional.ofNullable(oidToTopologyDataDefinition.get(id));
    }

    /**
     * Return a collection of all {@link TopologyDataDefinitionEntry}s.
     *
     * @return {@link Collection} of all {@link TopologyDataDefinitionEntry}s.
     */
    public Collection<TopologyDataDefinitionEntry> getAllTopologyDataDefinitions() {
        return oidToTopologyDataDefinition.entrySet().stream()
            .map(entry -> TopologyDataDefinitionEntry.newBuilder()
                .setId(entry.getKey())
                .setDefinition(entry.getValue())
                .build())
            .collect(Collectors.toList());
    }

    /**
     * Update an existing {@link TopologyDataDefinition} with a new value.
     *
     * @param id the OID of the existing {@link TopologyDataDefinition}.
     * @param updatedDefinition the new value of the {@link TopologyDataDefinition}.
     * @return Optional of {@link TopologyDataDefinition} if it exists or Optional.empty if not.
     */
    public Optional<TopologyDataDefinitionEntry> updateTopologyDataDefinition(long id,
                                                                    @Nonnull TopologyDataDefinition
                                                                    updatedDefinition) {
        if (!oidToTopologyDataDefinition.keySet().contains(id)) {
            logger.warn("Update failed: no TopologyEntityDefinition with OID {} found.", id);
            return Optional.empty();
        }
        oidToTopologyDataDefinition.put(id, updatedDefinition);
        logger.debug("Updated TopologyDataDefinition with OID {}.  New value: {}.", () -> id,
            () -> updatedDefinition);
        return Optional.of(TopologyDataDefinitionEntry.newBuilder()
            .setId(id)
            .setDefinition(updatedDefinition)
            .build());
    }

    /**
     * TODO implement restoring from Diags.
     *
     * @param collectedDiags previously collected diagnostics
     * @throws DiagnosticsException when an error is encountered.
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {

    }

    /**
     * TODO implement writing out Diags.
     *
     * @param appender an appender to put diagnostics to. String by string.
     * @throws DiagnosticsException when an error is encountered.
     */
    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {

    }

    @Nonnull
    @Override
    public String getFileName() {
        return TOPOLOGY_DATA_DEFINITION_DUMP_FILE;
    }

    /**
     * Delete the {@link TopologyDataDefinition} with the given OID.
     *
     * @param id the OID of the {@link TopologyDataDefinition} to delete.
     * @return boolean indicating whether or not the {@link TopologyDataDefinition} was deleted.
     */
    public boolean deleteTopologyDataDefinition(final long id) {
        if (!oidToTopologyDataDefinition.containsKey(id)) {
            logger.warn("Attempt to delete non-existing TopologyDataDefinition with OID {}.", id);
        }
        return oidToTopologyDataDefinition.remove(id) != null;
    }
}

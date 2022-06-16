package com.vmturbo.extractor.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Contains static definitions for allowed entity - to - entity relations to be included in the
 * extracted data.
 */
public class EntityRelationshipFilter {

    private EntityRelationshipFilter() {}

    /**
     * Static definition for the entity relationship whitelist.
     *
     * <p>There are two ways to add new entries to the whitelist:
     *     <ol>
     *         <li><code>.setSource(typeA).setDestination(typeB, typeC, ...)</code> if typeA should
     *         be related to typeB and typeC</li>
     *         <li><code>.allowAll(typeA)</code> if typeA should be allowed to relate to all entity
     *         types</li>
     *     </ol>
     * </p>
     */
    private static final EntityRelationshipWhitelist whitelist = new EntityRelationshipWhitelist()
            .setSource(EntityType.VIRTUAL_DATACENTER)
            .setDestination(EntityType.DATACENTER)
            .setSource(EntityType.STORAGE)
            .setDestination(EntityType.PHYSICAL_MACHINE, EntityType.VIRTUAL_MACHINE,
                    EntityType.DATACENTER)
            // TODO: verify that business_transaction and business_account
            .setSource(EntityType.SERVICE)
            .setDestination(EntityType.APPLICATION_COMPONENT, EntityType.BUSINESS_TRANSACTION,
                    EntityType.BUSINESS_ACCOUNT)
            // TODO: verify service_provider
            .setSource(EntityType.DATABASE_SERVER)
            .setDestination(EntityType.DATACENTER, EntityType.SERVICE_PROVIDER)
            // TODO: verify availability_zone
            .setSource(EntityType.VIRTUAL_MACHINE)
            .setDestination(EntityType.PHYSICAL_MACHINE, EntityType.DATACENTER, EntityType.STORAGE,
                    EntityType.VIRTUAL_VOLUME, EntityType.SERVICE_PROVIDER, EntityType.REGION,
                    EntityType.AVAILABILITY_ZONE)
            .setSource(EntityType.PHYSICAL_MACHINE)
            .setDestination(EntityType.VIRTUAL_MACHINE, EntityType.STORAGE, EntityType.DATACENTER,
                    EntityType.NETWORK)
            .setSource(EntityType.CONTAINER)
            .setDestination(EntityType.APPLICATION_COMPONENT)
            .setSource(EntityType.DATABASE)
            .setDestination(EntityType.SERVICE_PROVIDER)
            // TODO: verify both og the following entries
            .setSource(EntityType.REGION)
            .setDestination(EntityType.AVAILABILITY_ZONE)
            .setSource(EntityType.AVAILABILITY_ZONE)
            .setDestination(EntityType.REGION)
            .setSource(EntityType.COMPUTE_TIER)
            .setDestination(EntityType.SERVICE_PROVIDER)
            .setSource(EntityType.STORAGE_TIER)
            .setDestination(EntityType.SERVICE_PROVIDER)
            .setSource(EntityType.DATABASE_TIER)
            .setDestination(EntityType.SERVICE_PROVIDER)
            .setSource(EntityType.DATABASE_SERVER_TIER)
            .setDestination(EntityType.SERVICE_PROVIDER)
            .setSource(EntityType.VIRTUAL_VOLUME)
            .setDestination(EntityType.VIRTUAL_MACHINE, EntityType.SERVICE_PROVIDER)
            .setSource(EntityType.APPLICATION_COMPONENT)
            .setDestination(EntityType.SERVICE);

    /**
     * Provides the entity filter for each entity type. For example, if typeA should only be related
     * to typeB and typeC this method will return a predicate that matches typeB and typeC.
     *
     * @param entityType the type of the entity
     * @return a predicate that filters out any entity types that the above entity should not be
     *         related to in the exported data set.
     */
    public static Predicate<Integer> forType(int entityType) {
        return whitelist.forType(entityType);
    }

    /**
     * Models the whitelist.
     */
    private static class EntityRelationshipWhitelist {
        private final Map<Integer, Collection<Integer>> whitelist = new HashMap<>();
        private final List<Integer> allowAll = new ArrayList<>();

        /**
         * Sets the entity type for which related entity types need to be whitelisted.
         *
         * @param source any entity type
         * @return an intermediate whitelist that allows to define the whitelisted related entities
         */
        public IntermediateWhitelist setSource(EntityType source) {
            return new IntermediateWhitelist(this, source);
        }

        /**
         * Used to allow all relations for a specific entity type.
         *
         * @param entityType any entity type
         * @return the updated whitelist
         */
        public EntityRelationshipWhitelist allowAll(EntityType entityType) {
            allowAll.add(entityType.getNumber());
            return this;
        }

        /**
         * Used to get the related entities predicate for an entity type.
         *
         * @param entityType for which relationship filtering needs to happen
         * @return a predicate that disallows all non-whitelisted related entities
         */
        public Predicate<Integer> forType(int entityType) {

            if (allowAll.contains(entityType)) {
                return (type) -> true;
            }

            if (whitelist.containsKey(entityType)) {
                return whitelist.get(entityType)::contains;
            }

            return (type) -> false;
        }

        private void addToWhitelist(EntityType source, EntityType... destination) {
            this.whitelist
                    .computeIfAbsent(source.getNumber(), k -> new HashSet<>())
                    .addAll(Arrays
                            .stream(destination)
                            .map(EntityType::getNumber)
                            .collect(Collectors.toList()));
        }

        /**
         * Used for type safe construction of a whitelist.
         */
        private static class IntermediateWhitelist {
            private final EntityRelationshipWhitelist parent;
            private final EntityType source;

            /**
             * Models a whitelist and allows type safe construction.
             *
             * @param parent the whitelist object
             * @param source the "from" entity in an entity-to-entity relationship
             */
            IntermediateWhitelist(EntityRelationshipWhitelist parent, EntityType source) {
                this.parent = parent;
                this.source = source;
            }

            /**
             * Sets the whitelisted related entity types for a specific source defined by
             * {@link EntityRelationshipWhitelist#setSource}.
             *
             * @param types list of allowed related entity types
             * @return the updated whitelist
             */
            public EntityRelationshipWhitelist setDestination(EntityType... types) {
                this.parent.addToWhitelist(source, types);
                return this.parent;
            }
        }
    }
}

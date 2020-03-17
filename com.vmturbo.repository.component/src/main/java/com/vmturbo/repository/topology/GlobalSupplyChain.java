package com.vmturbo.repository.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.DocumentCreateOptions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.repository.graph.executor.ArangoDBExecutor;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.ResultsConverter;
import com.vmturbo.repository.service.ArangoSupplyChainRpcService;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyEntitiesException;


/**
 * In-memory representation of the global supply chain. We maintain
 * the entity attributes and the relationships needed to create the global
 * supply chain in memory as computing them from Arangodb is slower.
 *
 * We persist the relationships and the entity attributes in a separate collection
 * in Arangodb so that, on startup of repository, we can rebuild the in-memory
 * representation.
 */
public class GlobalSupplyChain implements DiagsRestorable {
    /**
     * The file name for the state of the {@link GlobalSupplyChainManager}. It's a string file,
     * so the "diags" extension is required for compatibility with {@link DiagsZipReader}.
     */
    public static final String GLOBAL_SUPPLY_CHAIN_DIAGS_FILE = "global-supply-chain";

    private static final Logger logger = LogManager.getLogger();

    private static final String GLOBAL_SUPPLY_CHAIN_ENTITY_INFO_DOC_KEY =
            "globalSupplyChainEntityInfoKey";

    private final GraphDBExecutor graphDbExecutor;

    /**
     *  The supply chain of SE numerical types.
     */
    final Multimap<Integer, Integer> providerRels = HashMultimap.create();

    /**
     * The id info of the topology this global supply chain is associated with.
     */
    private final TopologyID topologyId;

    /**
     *  Mapping from EntityState to Oid
     */
    private final Map<EntityState, Set<Long>> entityStateToOidsMap =
            new HashMap<>();

    /**
     * Mapping from EnvironmentType to Set(Oid)
     */
    private final Map<EnvironmentType, Set<Long>> envTypeToOidsMap =
            new HashMap<>();

    /**
     * Map from entityType to Oid.
     */
    final Map<Integer, Set<Long>> entityTypeToOidsMap =
            new HashMap<>();

    /**
     * Map from SE oid to its numerical type.
     */
    final Map<Long, Integer> oidToEntityType =
            new HashMap<>();

    /**
     * Map from oid of an SE which type is not yet known to the SE numerical
     * types that it is a provider of. Used for handling forward references,
     * when the provider DTO was not processed yet.
     */
    final Multimap<Long, Integer> unknownProvidersMap = HashMultimap.create();

    /**
     * A multimap that records the "provide" relationship.
     *
     * The key is an entity type, and the value is a list of providers for that entity type.
     *
     * For example:
     * <pre>
     *     {
     *         "VM": ["PM", "ST"]
     *     }
     * </pre>
     * The above example represents virtual machine entity type has two providers:
     * physical machine and storage.
     */
    private volatile Multimap<String, String> globalSupplyChainProviderRels;

    /**
     * Flag to check if the GlobalSupply chain is sealed. Once the supply chain is
     * sealed, it will become immutable.
     */
    private volatile boolean isSealed = false;


    /**
     * This variable holds the entities info we are going to store in the DB
     * so that we can quickly construct the global supply chain on repository
     * restart.
     *
     * This structure is created while processing the chunks. After the data is
     * successfully persisted in the DB, this structure is cleared to free up space.
     */
    private final Map<String, EntityInfo> globalSupplyChainInfos =
            new HashMap<>();

    public GlobalSupplyChain(@Nonnull final TopologyID topologyId,
                             @Nonnull final GraphDBExecutor graphDbExecutor) {
        this.topologyId = Objects.requireNonNull(topologyId);
        this.graphDbExecutor = Objects.requireNonNull(graphDbExecutor);
        this.globalSupplyChainProviderRels  = HashMultimap.create();
    }

    /**
     * Handle a partial collection of DTOs.
     * @param chunk a collection of DTOs that is only part of the whole topology.
     */
    public void processEntities(Collection<TopologyEntityDTO> chunk) {

        if (isSealed) {
            logger.warn("Cannot update. Global Supply chain already sealed for topology {}",
                    topologyId);
            return;
        }

        for (TopologyEntityDTO dto : chunk) {
            long oid = dto.getOid();
            int seType = dto.getEntityType();
            EntityState state = dto.getEntityState();
            EnvironmentType environmentType = dto.getEnvironmentType();
            oidToEntityType.put(oid, seType);
            entityStateToOidsMap.computeIfAbsent(state, k -> new HashSet<>()).add(oid);
            entityTypeToOidsMap.computeIfAbsent(seType, k -> new HashSet<>()).add(oid);
            envTypeToOidsMap.computeIfAbsent(environmentType,
                    k -> new HashSet<>()).add(oid);

            globalSupplyChainInfos.put(Long.toString(oid),
                    new EntityInfo(seType, state.getNumber(), environmentType.getNumber()));

            Collection<Integer> consumersTypes = unknownProvidersMap.get(oid);
            if (!consumersTypes.isEmpty()) {
                // the DTO is a provider but only now we found its type
                for (Integer consumerType : consumersTypes) {
                    providerRels.put(consumerType, seType);
                }
                unknownProvidersMap.removeAll(oid);
            }

            for (CommoditiesBoughtFromProvider provider : dto.getCommoditiesBoughtFromProvidersList()) {
                if (provider.hasProviderId()) {
                    Integer providerType = oidToEntityType.get(provider.getProviderId());
                    if (providerType != null) {
                        providerRels.put(seType, providerType);
                    }
                    else {
                        unknownProvidersMap.put(provider.getProviderId(), seType);
                    }
                }
            }

            // add connectedTo entity types to providers since we want to show a mix of consumes
            // and connected relationship in the global supply chain
            // Note: currently we don't want to show some cloud entity types, so we skip them
            if (!ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN.contains(seType)) {
                dto.getConnectedEntityListList().forEach(connectedEntity -> {
                    Integer providerType = connectedEntity.getConnectedEntityType();
                    if (!ArangoSupplyChainRpcService.IGNORED_ENTITY_TYPES_FOR_GLOBAL_SUPPLY_CHAIN.contains(providerType)) {
                        providerRels.put(seType, providerType);
                    }
                });
            }
        }
    }

    /**
     *  Prevent any further updates to the supply chain.
     */
    public void seal() {
        logger.info("Sealing global supply chain for {}", topologyId);
        isSealed = true;
        providerRels.asMap().forEach((seType, provTypes) -> {
            Set<String> repoProvTypes = provTypes.stream().map(UIEntityType::fromType)
                .map(UIEntityType::apiStr)
                .collect(Collectors.toSet());
            globalSupplyChainProviderRels.putAll(UIEntityType.fromType(seType).apiStr(),
                    repoProvTypes);
        });

        // clear the temp objects so they get GC'd.
        providerRels.clear();
        unknownProvidersMap.clear();
        oidToEntityType.clear();
    }

    /**
     * Persist the global supply chain in the database.
     *
     */
    public void store() throws TopologyEntitiesException {
        // Store the entities and relationships in separate collections.
        if (!isSealed) {
            logger.warn("Not yet sealed");
            return;
        }

        logger.info("Storing global supply chain in db for: {}", topologyId);
        try {
            DocumentCreateOptions documentCreateOptions =
                    new DocumentCreateOptions();
            documentCreateOptions.returnNew(false);
            documentCreateOptions.waitForSync(true);

            GlobalSupplyChainRelationships supplyChainRels =
                    new GlobalSupplyChainRelationships(globalSupplyChainProviderRels);
            graphDbExecutor.insertNewDocument(supplyChainRels.convertToDocument(),
                getFullCollectionName(ArangoDBExecutor.GLOBAL_SUPPLY_CHAIN_RELS_COLLECTION),
                documentCreateOptions);

            final BaseDocument baseDocument = new BaseDocument();
            baseDocument.setKey(GLOBAL_SUPPLY_CHAIN_ENTITY_INFO_DOC_KEY);
            final Map<String, Object> map = globalSupplyChainInfos.entrySet().stream()
                    .collect(Collectors.toMap(entry -> entry.getKey(),
                            entry -> (new Gson()).toJson(entry.getValue())));
            baseDocument.setProperties(map);
            graphDbExecutor.insertNewDocument(baseDocument,
                getFullCollectionName(ArangoDBExecutor.GLOBAL_SUPPLY_CHAIN_ENTITIES_COLLECTION),
                documentCreateOptions);

        } catch (ArangoDBException e) {
            throw new TopologyEntitiesException(e);
        }

        globalSupplyChainInfos.clear();
    }

    /**
     * Create global supply chain by loading from the database.
     */
    public boolean loadFromDatabase() {

        if (isSealed) {
            logger.warn("Can't load. Already sealed.");
            return false;
        }

        logger.info("Loading global supply chain from db for: {}", topologyId);

        globalSupplyChainProviderRels = graphDbExecutor.getSupplyChainRels().getRelationships();

        try {

            BaseDocument baseDocument = loadDocumentFromDb();
            baseDocument.getProperties().entrySet()
                    .forEach(entry -> {
                        Long oid = Long.valueOf(entry.getKey());
                        EntityInfo info = (new Gson()).fromJson((String)entry.getValue(), EntityInfo.class);
                        entityTypeToOidsMap.computeIfAbsent(info.type,
                                k -> new HashSet<>()).add(oid);
                        entityStateToOidsMap.computeIfAbsent(EntityState.forNumber(info.state),
                                k -> new HashSet<>()).add(oid);
                        envTypeToOidsMap.computeIfAbsent(EnvironmentType.forNumber(info.envType),
                                k -> new HashSet<>()).add(oid);
                    });
        } catch (ArangoDBException e) {
            logger.error("Failed to load supply chain entities info", e);
            return false;
        }

        isSealed = true;
        return true;
    }

    private BaseDocument loadDocumentFromDb()
            throws ArangoDBException {
        return graphDbExecutor.getDocument(GLOBAL_SUPPLY_CHAIN_ENTITY_INFO_DOC_KEY,
            getFullCollectionName(ArangoDBExecutor.GLOBAL_SUPPLY_CHAIN_ENTITIES_COLLECTION));
    }

    /**
     * Get full collection name by concatenating given collection name with collection name suffix.
     *
     * @param collection Given collection name.
     * @return Constructed full collection name.
     */
    private String getFullCollectionName(String collection) {
        return collection + topologyId.toCollectionNameSuffix();
    }

    /**
     * Create SupplyChainNodes from the Global Supply Chain.
     *
     * @param filter Filter to be applied on the supply chain.
     *
     * @return Mapping from EntityType to SupplyChainNode.
     */
    public Map<String, SupplyChainNode> toSupplyChainNodes(@Nonnull GlobalSupplyChainFilter filter) {

        if (!isSealed) {
            logger.warn("Cannot return supply chain as it is not yet sealed.");
            return Collections.emptyMap();
        }

        Set<Long> allowedOids = new HashSet<>();
        if (filter.getEnvironmentType().isPresent()) {
            allowedOids.addAll(envTypeToOidsMap.get(filter.getEnvironmentType().get()));
        } else {
            allowedOids.addAll(envTypeToOidsMap.values().stream()
                    .flatMap(oids -> oids.stream())
                    .collect(Collectors.toSet()));
        }
        if (!filter.getAllowedOids().isEmpty()) {
            allowedOids.retainAll(filter.getAllowedOids());
        }

        // Create supply chain nodes.
        Map<String, SupplyChainNode.Builder> entityTypeToSupplyChainNodesMap =
            entityTypeToOidsMap.entrySet()
                .stream()
                .filter(entityType -> !filter.getIgnoredEntityTypes().contains(entityType))
                .collect(Collectors.toMap(
                    entry -> UIEntityType.fromType(entry.getKey()).apiStr(),
                    entry -> {
                        SupplyChainNode.Builder nodeBuilder = SupplyChainNode.newBuilder();
                        nodeBuilder.setEntityType(UIEntityType.fromType(entry.getKey()).apiStr());
                        // create a mapping from EntityState to Oids
                        nodeBuilder.putAllMembersByState(
                            entityStateToOidsMap.entrySet().stream()
                                .collect(Collectors.toMap(
                                    entityStateOidsEntry -> entityStateOidsEntry.getKey().getNumber(),
                                    entityStateOidsEntry -> {
                                        MemberList.Builder builder = MemberList.newBuilder();
                                        Set<Long> oidsToAdd =
                                                new HashSet<>(entry.getValue());
                                        oidsToAdd.retainAll(allowedOids);
                                        oidsToAdd.retainAll(entityStateOidsEntry.getValue());
                                        builder.addAllMemberOids(oidsToAdd);
                                        return builder.build();
                                    })));
                        return nodeBuilder;
                }));

        final Multimap<String, String> consumerRels =
                Multimaps.invertFrom(globalSupplyChainProviderRels, HashMultimap.create());
        ResultsConverter.fillNodeRelationships(entityTypeToSupplyChainNodesMap,
                globalSupplyChainProviderRels, consumerRels,
                GraphCmd.SupplyChainDirection.PROVIDER);
        ResultsConverter.fillNodeRelationships(entityTypeToSupplyChainNodesMap, consumerRels,
                globalSupplyChainProviderRels, GraphCmd.SupplyChainDirection.CONSUMER);

        return entityTypeToSupplyChainNodesMap.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().build()));
    }

    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        appender.appendString(ComponentGsonFactory.createGsonNoPrettyPrint()
                .toJson(globalSupplyChainProviderRels.asMap()));

        Map<Long, EntityInfo> oidToEntityInfoMap =
                new HashMap<>();
        try {
            BaseDocument baseDocument = loadDocumentFromDb();
            baseDocument.getProperties().entrySet()
                    .forEach(entry -> {
                        oidToEntityInfoMap.put(Long.valueOf(entry.getKey()),
                                (new Gson()).fromJson((String)entry.getValue(),
                                        EntityInfo.class));
                    });
        } catch (ArangoDBException e) {
            logger.error("Error loading global supply chain document from db");

        }
        appender.appendString(ComponentGsonFactory.createGsonNoPrettyPrint()
                .toJson(oidToEntityInfoMap));
    }

    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags)
            throws DiagnosticsException {

        Map<String, Collection<String>> map =
                ComponentGsonFactory.createGsonNoPrettyPrint().fromJson(collectedDiags.get(0),
                        new TypeToken<Map<String, Collection<String>>>(){}.getType());
        Multimap<String, String> multimap = HashMultimap.create();
        map.forEach(multimap::putAll);
        globalSupplyChainProviderRels = multimap;

        Map<String, EntityInfo> oidToEntityInfoMap =
                ComponentGsonFactory.createGsonNoPrettyPrint().fromJson(collectedDiags.get(1),
                        new TypeToken<Map<String, EntityInfo>>(){}.getType());

        oidToEntityInfoMap.entrySet().forEach(entry -> {
            Long oid = Long.valueOf(entry.getKey());
            EntityInfo info = entry.getValue();
            entityTypeToOidsMap.computeIfAbsent(info.type,
                    k -> new HashSet<>()).add(oid);
            entityStateToOidsMap.computeIfAbsent(EntityState.forNumber(info.state),
                    k -> new HashSet<>()).add(oid);
            envTypeToOidsMap.computeIfAbsent(EnvironmentType.forNumber(info.envType),
                    k -> new HashSet<>()).add(oid);
        });
    }

    @Nonnull
    @Override
    public String getFileName() {
        return GLOBAL_SUPPLY_CHAIN_DIAGS_FILE;
    }

    private class EntityInfo{

        public final int type;
        public final int state;
        public final int envType;

        EntityInfo(int entityType, int entityState, int environmentType) {
           this.type = entityType;
           this.state = entityState;
           this.envType = environmentType;
        }
    }
}

package com.vmturbo.extractor.export;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.patchers.GroupPrimitiveFieldsOnGroupingPatcher;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher;
import com.vmturbo.extractor.schema.json.export.Entity;
import com.vmturbo.extractor.schema.json.export.ExportedObject;
import com.vmturbo.extractor.schema.json.export.Group;
import com.vmturbo.extractor.search.EnumUtils.EntityStateUtils;
import com.vmturbo.extractor.search.EnumUtils.EntityTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.EnvironmentTypeUtils;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.TopologyWriterBase;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.search.metadata.SearchMetadataMapping;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Writer that extracts entities data from a topology and publishes to a kafka topic.
 */
public class DataExtractionWriter extends TopologyWriterBase {

    private static final Logger logger = LogManager.getLogger();

    private final List<Entity> entities = new ArrayList<>();
    private final ExtractorKafkaSender extractorKafkaSender;
    private final DataExtractionFactory dataExtractionFactory;
    private final MetricsExtractor metricsExtractor;
    private final PrimitiveFieldsOnTEDPatcher attrsExtractor;
    private final GroupPrimitiveFieldsOnGroupingPatcher groupAttrsExtractor;
    private String formattedTopologyCreationTime;

    /**
     * Create a new writer instance.
     *
     * @param extractorKafkaSender used to send entities to a kafka topic
     * @param dataExtractionFactory factory for providing instances of different extractors
     */
    public DataExtractionWriter(@Nonnull ExtractorKafkaSender extractorKafkaSender,
            @Nonnull DataExtractionFactory dataExtractionFactory) {
        super(null, null);
        this.extractorKafkaSender = extractorKafkaSender;
        this.dataExtractionFactory = dataExtractionFactory;
        this.metricsExtractor = dataExtractionFactory.newMetricsExtractor();
        this.attrsExtractor = dataExtractionFactory.newAttrsExtractor();
        this.groupAttrsExtractor = dataExtractionFactory.newGroupAttrsExtractor();
    }

    @Override
    public Consumer<TopologyEntityDTO> startTopology(final TopologyInfo topologyInfo,
            final WriterConfig config, final MultiStageTimer timer)
            throws IOException, UnsupportedDialectException, SQLException, InterruptedException {
        super.startTopology(topologyInfo, config, timer);
        this.formattedTopologyCreationTime = ExportUtils.getFormattedDate(topologyInfo.getCreationTime());
        return this::writeEntity;
    }

    @Override
    public boolean requireFullSupplyChain() {
        return true;
    }

    @Override
    protected void writeEntity(final TopologyEntityDTO e) {
        // to be consistent with reporting
        if (EntityTypeUtils.protoIntToDb(e.getEntityType(), null) == null) {
            logger.error("Cannot map entity type {} for entity oid {}; skipping",
                    e.getEntityType(), e.getOid());
            return;
        }

        final Entity entity = new Entity();
        entity.setOid(e.getOid());
        entity.setName(e.getDisplayName());
        // use proto db str (rather than api str) to keep consistent with reporting
        entity.setType(ExportUtils.getEntityTypeJsonKey(e.getEntityType()));
        entity.setEnvironment(EnvironmentTypeUtils.protoToDb(e.getEnvironmentType()).getLiteral());
        entity.setState(EntityStateUtils.protoToDb(e.getEntityState()).getLiteral());
        // metrics
        entity.setMetric(metricsExtractor.extractMetrics(e, config.reportingCommodityWhitelist()));
        // attrs
        Map<String, Object> attrs = attrsExtractor.extractAttrs(e);
        // remove vendor_id for data extraction since it is now represented in attrs.targets
        // we don't want to remove it reporting attrs for now for safety.
        if (attrs != null) {
            attrs.remove(SearchMetadataMapping.PRIMITIVE_VENDOR_ID.getJsonKeyName());
        }
        entity.setAttrs(attrs);
        // cache entities to be sent to kafka later
        entities.add(entity);
    }

    @Override
    public int finish(final DataProvider dataProvider) {
        // extract all groups
        final List<ExportedObject> exportedGroups = dataProvider.getAllGroups()
                .map(grouping -> {
                    final Group group = new Group();
                    group.setOid(grouping.getId());
                    group.setName(grouping.getDefinition().getDisplayName());
                    group.setType(ExportUtils.getGroupTypeJsonKey(grouping.getDefinition().getType()));
                    group.setAttrs(groupAttrsExtractor.extractAttrs(grouping));

                    ExportedObject exportedObject = new ExportedObject();
                    exportedObject.setTimestamp(formattedTopologyCreationTime);
                    exportedObject.setGroup(group);
                    return exportedObject;
                }).collect(Collectors.toList());

        final Optional<RelatedEntitiesExtractor> relatedEntitiesExtractor =
                dataExtractionFactory.newRelatedEntitiesExtractor();
        final Optional<TopDownCostExtractor> topDownCostExtractor =
                dataExtractionFactory.newTopDownCostExtractor();
        final Optional<BottomUpCostExtractor> bottomUpCostExtractor =
                dataExtractionFactory.newBottomUpCostExtractor();

        // set related entities and related groups
        final String relatedStageLabel = "Populate related entities and groups";
        logger.info("Starting stage: {}", relatedStageLabel);
        timer.start(relatedStageLabel);
        final List<ExportedObject> exportedObjects = entities.parallelStream()
                .map(entity -> {
                    relatedEntitiesExtractor.ifPresent(extractor -> entity.setRelated(
                        extractor.extractRelatedEntities(entity.getOid())));
                    topDownCostExtractor.flatMap(extractor -> extractor.getExpenses(entity.getOid()))
                        .ifPresent(entity::setAccountExpenses);
                    // set bottom up cost
                    bottomUpCostExtractor.ifPresent(extractor ->
                            entity.setCost(extractor.getCost(entity.getOid())));

                    final ExportedObject exportedObject = new ExportedObject();
                    exportedObject.setTimestamp(formattedTopologyCreationTime);
                    exportedObject.setEntity(entity);
                    return exportedObject;
                }).collect(Collectors.toList());
        timer.stop();

        // add groups together with entities
        exportedObjects.addAll(exportedGroups);

        // send entities and groups to kafka
        final String kafkaStageLabel = "Send entities and groups to Kafka";
        logger.info("Starting stage: {}", kafkaStageLabel);
        timer.start(kafkaStageLabel);
        final int successCount = extractorKafkaSender.send(exportedObjects);
        timer.stop();
        return successCount;
    }
}

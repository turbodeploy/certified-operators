package com.vmturbo.extractor.action;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.action.PendingActionWriter.IActionWriter;
import com.vmturbo.extractor.export.DataExtractionFactory;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.export.RelatedEntitiesExtractor;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.export.ExportedObject;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Extract pending actions in AO and send to Kafka.
 */
class DataExtractionPendingActionWriter implements IActionWriter {

    private static final Logger logger = LogManager.getLogger();

    private Map<Long, Policy> policyById = new HashMap<>();
    private final ExtractorKafkaSender extractorKafkaSender;
    private final Optional<RelatedEntitiesExtractor> relatedEntitiesExtractor;
    private final TopologyGraph<SupplyChainEntity> topologyGraph;
    private ActionConverter actionConverter;
    private final MutableLong lastActionExtraction;
    private final List<Action> actions;
    private final String exportTimeFormatted;
    private final long exportTimeInMillis;

    DataExtractionPendingActionWriter(ExtractorKafkaSender extractorKafkaSender,
            DataExtractionFactory dataExtractionFactory, DataProvider dataProvider, Clock clock,
            MutableLong lastActionExtraction, ActionConverter actionConverter) {
        this.extractorKafkaSender = extractorKafkaSender;
        this.topologyGraph = dataProvider.getTopologyGraph();
        this.actionConverter = actionConverter;
        this.relatedEntitiesExtractor = dataExtractionFactory.newRelatedEntitiesExtractor(dataProvider);
        this.lastActionExtraction = lastActionExtraction;
        this.exportTimeInMillis = clock.millis();
        this.exportTimeFormatted = ExportUtils.getFormattedDate(exportTimeInMillis);
        this.actions = new ArrayList<>();
    }

    @Override
    public boolean requirePolicy() {
        return true;
    }

    @Override
    public void acceptPolicy(Map<Long, Policy> policyById) {
        this.policyById = policyById;
    }

    @Override
    public void recordAction(ActionOrchestratorAction aoAction) {
        actions.add(actionConverter.makeExportedAction(aoAction.getActionSpec(),
                topologyGraph, policyById, relatedEntitiesExtractor));
    }

    @Override
    public void write(MultiStageTimer timer) {
        final List<ExportedObject> exportedObjects = actions.stream()
                .map(action -> {
                    ExportedObject exportedObject = new ExportedObject();
                    exportedObject.setTimestamp(exportTimeFormatted);
                    exportedObject.setAction(action);
                    return exportedObject;
                }).collect(Collectors.toList());
        // send to Kafka
        timer.start("Send actions to Kafka");
        extractorKafkaSender.send(exportedObjects);
        timer.stop();
        // update last extraction time
        lastActionExtraction.setValue(exportTimeInMillis);
    }
}

package com.vmturbo.extractor.action;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.action.PendingActionWriter.IActionWriter;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.export.ExportedObject;

/**
 * Extract pending actions in AO and send to Kafka.
 */
class DataExtractionPendingActionWriter implements IActionWriter {

    private static final Logger logger = LogManager.getLogger();

    private final ExtractorKafkaSender extractorKafkaSender;
    private final ActionConverter actionConverter;
    private final MutableLong lastActionExtraction;

    private final List<ActionSpec> actions = new ArrayList<>();

    private final String exportTimeFormatted;
    private final long exportTimeInMillis;

    DataExtractionPendingActionWriter(ExtractorKafkaSender extractorKafkaSender, Clock clock,
            MutableLong lastActionExtraction, ActionConverter actionConverter) {
        this.extractorKafkaSender = extractorKafkaSender;
        this.actionConverter = actionConverter;
        this.lastActionExtraction = lastActionExtraction;
        this.exportTimeInMillis = clock.millis();
        this.exportTimeFormatted = ExportUtils.getFormattedDate(exportTimeInMillis);
    }

    @Override
    public boolean requirePolicy() {
        return true;
    }

    @Override
    public void recordAction(ActionOrchestratorAction aoAction) {
        actions.add(aoAction.getActionSpec());
    }

    @Override
    public void write(MultiStageTimer timer) {

        timer.start("Converting actions to export format");
        Collection<Action> exportedActions = actionConverter.makeExportedActions(actions);

        final List<ExportedObject> exportedObjects = exportedActions.stream()
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

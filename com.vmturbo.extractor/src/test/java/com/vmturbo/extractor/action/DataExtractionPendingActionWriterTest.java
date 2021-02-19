package com.vmturbo.extractor.action;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.export.ExportedObject;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test for {@link DataExtractionPendingActionWriter}.
 */
public class DataExtractionPendingActionWriterTest {

    private static final Logger logger = LogManager.getLogger();

    private static final MultiStageTimer timer = new MultiStageTimer(logger);

    private static final long vm1 = 10L;
    private static final long host1 = 20L;
    private static final long storage1 = 30L;

    private static final ActionOrchestratorAction ACTION = ActionOrchestratorAction.newBuilder()
            .setActionId(1001L)
            .setActionSpec(ActionSpec.newBuilder()
                    .setActionState(ActionState.READY)
                    .setActionMode(ActionMode.MANUAL)
                    .setCategory(ActionCategory.EFFICIENCY_IMPROVEMENT)
                    .setSeverity(Severity.MAJOR)
                    .setDescription("Move vm1 from host1 to host2")
                    .setRecommendation(ActionDTO.Action.newBuilder()
                            .setId(1001L)
                            .setInfo(ActionInfo.newBuilder()
                                    .setMove(Move.newBuilder()
                                            .setTarget(ActionEntity.newBuilder()
                                                    .setId(vm1)
                                                    .setType(EntityType.VIRTUAL_MACHINE_VALUE))))
                            .setExecutable(true)
                            .setSupportingLevel(SupportLevel.SUPPORTED)
                            .setDeprecatedImportance(0)
                            .setExplanation(Explanation.getDefaultInstance())))
            .build();

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private ExtractorKafkaSender extractorKafkaSender = mock(ExtractorKafkaSender.class);

    private MutableLong lastWrite = new MutableLong(0);

    private ActionConverter actionConverter = mock(ActionConverter.class);

    private DataExtractionPendingActionWriter writer;

    private List<ExportedObject> actionsCapture;

    /**
     * Common setup code before each test.
     */
    @Before
    public void setup() {
        // capture actions sent to kafka
        this.actionsCapture = new ArrayList<>();
        doAnswer(inv -> {
            Collection<ExportedObject> exportedObjects = inv.getArgumentAt(0, Collection.class);
            if (exportedObjects != null) {
                actionsCapture.addAll(exportedObjects);
            }
            return null;
        }).when(extractorKafkaSender).send(any());

        writer = spy(new DataExtractionPendingActionWriter(extractorKafkaSender, clock, lastWrite, actionConverter));
    }

    /**
     * Test that compound move action is extracted correctly. The action contains 4 providers
     * changes: 1 host move + 3 storage moves.
     */
    @Test
    public void testExtractAction() {
        Action exportedAction = new Action();
        exportedAction.setOid(1L);
        when(actionConverter.makeExportedActions(any()))
            .thenReturn(Collections.singletonList(exportedAction));

        // extract
        writer.recordAction(ACTION);
        writer.write(timer);

        // verify
        assertThat(actionsCapture.size(), is(1));

        final ExportedObject obj = actionsCapture.get(0);
        final Action action = obj.getAction();
        assertThat(action, is(exportedAction));
    }
}
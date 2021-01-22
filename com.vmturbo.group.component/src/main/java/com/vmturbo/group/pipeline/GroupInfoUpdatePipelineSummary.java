package com.vmturbo.group.pipeline;

import java.time.Clock;
import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.common.pipeline.PipelineSummary;
import com.vmturbo.components.common.pipeline.Stage;

/**
 * The summary of {@link GroupInfoUpdatePipeline}, containing basic information.
 */
public class GroupInfoUpdatePipelineSummary extends PipelineSummary {

    private final GroupInfoUpdatePipelineContext context;

    protected GroupInfoUpdatePipelineSummary(@Nonnull Clock clock,
            @Nonnull GroupInfoUpdatePipelineContext context,
            @Nonnull List<Stage> stages) {
        super(clock, stages);
        this.context = context;
    }

    @Override
    protected String getPreamble() {
        return FormattedString.format("{}\nTopology ID: {}\n",
                context.getPipelineName(), context.getTopologyId());
    }
}

package com.vmturbo.mediation.azure.pricing.stages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline.Stage;
import com.vmturbo.mediation.azure.pricing.resolver.ResolvedMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;

/**
 * Stage to take a collection of resolved meters and transform them into a pricing workspace,
 * including a map of resolved meters by meter type.
 *
 * @param <E> The enum for the probe stages that apply to this particular kind of discovery.
 */
public class RegroupByTypeStage<E extends ProbeStageEnum>
        extends Stage<Collection<ResolvedMeter>, PricingWorkspace, E> {
    /**
     * Constructor for a pricing pipeline stage.
     *
     * @param probeStage The enum value for this probe stage in discovery or validation.
     */
    public RegroupByTypeStage(E probeStage) {
        super(probeStage);
    }

    @NotNull
    @Override
    protected StageResult<PricingWorkspace> executeStage(@NotNull Collection<ResolvedMeter> input)
            throws PipelineStageException {
        Map<MeterType, List<ResolvedMeter>> metersByType = new HashMap<>();

        input.forEach(resolvedMeter -> {
            metersByType
                .computeIfAbsent(resolvedMeter.getDescriptor().getType(), k -> new ArrayList<>())
                .add(resolvedMeter);
        });

        final String summary = String.format("%d meters grouped", input.size());
        final StringBuilder sb = new StringBuilder("Resolved meters by Type:\n");

        metersByType.forEach((key, value) -> {
            sb.append(String.format("\n%d\t%s", value.size(), key.toString()));
        });

        getStageInfo().ok(summary).longExplanation(sb.toString());

        return StageResult.withResult(new PricingWorkspace(metersByType))
            .andStatus(Status.success(summary));
    }
}

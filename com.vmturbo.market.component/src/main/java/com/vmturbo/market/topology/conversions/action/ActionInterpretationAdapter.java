package com.vmturbo.market.topology.conversions.action;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.market.topology.conversions.CommodityConverter;
import com.vmturbo.market.topology.conversions.CommodityIndex;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;

/**
 * An adapter for {@link com.vmturbo.market.topology.conversions.ActionInterpreter} that helps
 * interpret a specific market action into a specific platform action.
 *
 * <p/>TODO (roman, June 28 2021): Expand the framework beyond resize actions into all other action
 * types.
 *
 * @param <I> The input M2 action type.
 * @param <O> The output XL action type.
 * @param <E> The output XL explanation type.
 */
public abstract class ActionInterpretationAdapter<I, O, E> {
    protected final Logger logger = LogManager.getLogger(getClass());
    protected final CommodityConverter commodityConverter;
    protected final CommodityIndex commodityIndex;
    protected final Map<Long, TraderTO> oidToProjectedTraderTOMap;

    protected ActionInterpretationAdapter(final CommodityConverter commodityConverter,
            final CommodityIndex commodityIndex,
            final Map<Long, TraderTO> oidToProjectedTraderTOMap) {
        this.commodityConverter = commodityConverter;
        this.commodityIndex = commodityIndex;
        this.oidToProjectedTraderTOMap = oidToProjectedTraderTOMap;
    }

    /**
     * Interpret an analysis action into the output action format.
     *
     * @param analysisAction The input analysis action.
     * @param projectedTopology The projected topology after all actions are taken.
     * @return An optional containing the interpreted action, or an empty optional if this action
     *         should be dropped.
     */
    @Nonnull
    public abstract Optional<O> interpret(@Nonnull I analysisAction, @Nonnull Map<Long, ProjectedTopologyEntity> projectedTopology);

    /**
     * Interpret an analysis action into the output explanation format.
     *
     * @param analysisAction The output analysis action.
     * @return The interpreted explanation.
     */
    @Nonnull
    public abstract E interpretExplanation(@Nonnull I analysisAction);

    protected ActionEntity createActionEntity(final long id, final int entityType, EnvironmentType environmentType) {
        return ActionEntity.newBuilder()
                .setId(id)
                .setType(entityType)
                .setEnvironmentType(environmentType)
                .build();
    }

}

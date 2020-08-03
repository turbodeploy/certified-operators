package com.vmturbo.action.orchestrator.store;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Converter;

import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * Convert a ActionDTO.Action to/from a byte blob for serialization of the protobuf to the database.
 */
public class MarketActionConverter implements Converter<byte[], Action> {
    private static final Logger logger = LogManager.getLogger();

    @Override
    public Action from(byte[] specBlob) {
        try {
            return patch(Action.parseFrom(specBlob));
        } catch (InvalidProtocolBufferException e) {
            logger.error("Failed to convert specBlob to ActionDTO.Action: ", e);
            return null;
        }
    }

    @Override
    public byte[] to(Action actionDTO) {
        return actionDTO.toByteArray();
    }

    @Override
    public Class<byte[]> fromType() {
        return byte[].class;
    }

    @Override
    public Class<Action> toType() {
        return Action.class;
    }

    /**
     * Update the action with values from deprecated fields, if necessary.
     * I.e. when a deprecated field has value but there is a matching new field that doesn't.
     * Specifically reason commodities are patched.
     * TODO We should turn back from this road to perdition and STOP serializing blobs.
     * To have an ability to perform installation-time migration in SQL instead of doing it on the fly.
     *
     * @param dbAction deserialized (from rdbms) action dto
     * @return possibly modified action with deprecated fields' contents migrated
     */
    @Nonnull
    private static Action patch(@Nonnull Action dbAction) {
        if (dbAction.hasExplanation()) {
            Action.Builder patched = null;
            Explanation exp = dbAction.getExplanation();
            if (exp.hasMove()) {
                for (int i = 0; i < exp.getMove().getChangeProviderExplanationCount(); ++i) {
                    ChangeProviderExplanation movePart = exp.getMove()
                                    .getChangeProviderExplanation(i);
                    patched = patchCompliance(dbAction, patched, i, movePart);
                    patched = patchCongestion(dbAction, patched, i, movePart);
                    patched = patchEfficiency(dbAction, patched, i, movePart);
                }
            } else if (exp.hasReconfigure()) {
                patched = patchReconfigure(dbAction, patched, exp.getReconfigure());
            }
            if (patched != null) {
                return patched.build();
            }
        }
        return dbAction;
    }

    @Nullable
    @SuppressWarnings("deprecation")
    private static Action.Builder patchCompliance(@Nonnull Action dbAction,
                                                  @Nullable Action.Builder patched,
                                                  int movePartIndex,
                                                  @Nonnull ChangeProviderExplanation movePart) {
        if (movePart.hasCompliance()) {
            Compliance compliance = movePart.getCompliance();
            patched = copyCommodityTypes(dbAction,
                               patched,
                               compliance.getDeprecatedMissingCommoditiesCount(),
                               compliance.getMissingCommoditiesCount(),
                               (Action.Builder ab) -> ab
                                   .getExplanationBuilder().getMoveBuilder()
                                   .getChangeProviderExplanationBuilder(movePartIndex)
                                   .getComplianceBuilder(),
                               compliance.getDeprecatedMissingCommoditiesList(),
                               Compliance.Builder::addMissingCommodities);
        }
        return patched;
    }

    @Nullable
    @SuppressWarnings("deprecation")
    private static Action.Builder patchCongestion(@Nonnull Action dbAction,
                                                  @Nullable Action.Builder patched,
                                                  int movePartIndex,
                                                  @Nonnull ChangeProviderExplanation movePart) {
        if (movePart.hasCongestion()) {
            Congestion congestion = movePart.getCongestion();
            Function<Action.Builder, Congestion.Builder> congestionBuilderCreator =
                            (Action.Builder ab) -> ab
                                .getExplanationBuilder().getMoveBuilder()
                                .getChangeProviderExplanationBuilder(movePartIndex)
                                .getCongestionBuilder();
            patched = copyCommodityTypes(dbAction,
                               patched,
                               congestion.getDeprecatedCongestedCommoditiesCount(),
                               congestion.getCongestedCommoditiesCount(),
                               congestionBuilderCreator,
                               congestion.getDeprecatedCongestedCommoditiesList(),
                               Congestion.Builder::addCongestedCommodities);
        }
        return patched;
    }

    @Nullable
    @SuppressWarnings("deprecation")
    private static Action.Builder patchEfficiency(@Nonnull Action dbAction,
                                                  @Nullable Action.Builder patched,
                                                  int movePartIndex,
                                                  @Nonnull ChangeProviderExplanation movePart) {
        if (movePart.hasEfficiency()) {
            Efficiency efficiency = movePart.getEfficiency();
            Function<Action.Builder, Efficiency.Builder> efficiencyBuilderCreator =
                            (Action.Builder ab) -> ab
                                .getExplanationBuilder()
                                .getMoveBuilder()
                                .getChangeProviderExplanationBuilder(movePartIndex)
                                .getEfficiencyBuilder();
            patched = copyCommodityTypes(dbAction,
                               patched,
                               efficiency.getDeprecatedUnderUtilizedCommoditiesCount(),
                               efficiency.getUnderUtilizedCommoditiesCount(),
                               efficiencyBuilderCreator,
                               efficiency.getDeprecatedUnderUtilizedCommoditiesList(),
                               Efficiency.Builder::addUnderUtilizedCommodities);
        }
        return patched;
    }

    @Nullable
    @SuppressWarnings("deprecation")
    private static Action.Builder patchReconfigure(@Nonnull Action dbAction,
                                                  @Nullable Action.Builder patched,
                                                  @Nonnull ReconfigureExplanation reconfigure) {
        return copyCommodityTypes(dbAction,
                           patched,
                           reconfigure.getDeprecatedReconfigureCommodityCount(),
                           reconfigure.getReconfigureCommodityCount(),
                           (Action.Builder ab) -> ab.getExplanationBuilder().getReconfigureBuilder(),
                           reconfigure.getDeprecatedReconfigureCommodityList(),
                           ReconfigureExplanation.Builder::addReconfigureCommodity);
    }

    @Nullable
    private static <ContainerDto, ContainerDtoBuilder> Action.Builder
            copyCommodityTypes(@Nonnull Action dbAction,
                               @Nullable Action.Builder patched,
                               int deprecatedSize,
                               int newSize,
                               @Nonnull Function<Action.Builder, ContainerDtoBuilder> builderCreator,
                               @Nonnull List<CommodityType> types,
                               @Nonnull BiConsumer<ContainerDtoBuilder, ReasonCommodity> reasonSetter) {
        // migrate when old values exist but new do not
        if (deprecatedSize > 0 && newSize == 0) {
            // lazily create the outer and inner builders
            patched = patched == null ? dbAction.toBuilder() : patched;
            ContainerDtoBuilder builder = builderCreator.apply(patched);
            for (CommodityType ct : types) {
                reasonSetter.accept(builder, ReasonCommodity.newBuilder().setCommodityType(ct).build());
            }
        }
        return patched;
    }

}

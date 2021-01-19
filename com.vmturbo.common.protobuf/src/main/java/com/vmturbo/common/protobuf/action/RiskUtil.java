package com.vmturbo.common.protobuf.action;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.TRANSLATION_PATTERN;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.TRANSLATION_PREFIX;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Utility class for action risk.
 */
public class RiskUtil {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Commodity Types related to a policy.
     */
    public static final Set<Integer> POLICY_COMMODITY_TYPES = ImmutableSet.of(CommodityType.SEGMENTATION_VALUE,
        CommodityType.SOFTWARE_LICENSE_COMMODITY_VALUE);

    private RiskUtil() {
    }

    /**
     * Creates the risk description for an action.
     *
     * @param actionSpec              the object containing the action.
     * @param policyGetter            the function that gets the policy object with the input oid. The null
     *                                if the policy cannot be found.
     * @param entityDisplayNameGetter the function that gets the display name for an entity with
     *                                input oid. It returns null of such entity does not exists.
     * @return the risk description for the action.
     * @throws UnsupportedActionException If the type of the action is not supported.
     */
    public static String createRiskDescription(@Nonnull final ActionDTO.ActionSpec actionSpec,
           @Nonnull final Function<Long, PolicyDTO.Policy> policyGetter,
           @Nonnull final Function<Long, String> entityDisplayNameGetter) throws UnsupportedActionException {
        final Optional<String> policyId = tryExtractPlacementPolicyId(actionSpec.getRecommendation());
        if (policyId.isPresent()) {
            final ActionDTO.ActionEntity entity =
                ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation());
            final long policyOid = Long.parseLong(policyId.get());

            final Optional<PolicyDTO.Policy> policy =
                Optional.ofNullable(policyGetter.apply(policyOid));
            if (!policy.isPresent()) {
                return actionSpec.getExplanation();
            }
            if (actionSpec.getRecommendation().getExplanation().hasProvision()) {
                return String.format("%s violation", policy.get().getPolicyInfo().getDisplayName());
            } else {
                if (actionSpec.getRecommendation().getInfo().getReconfigure().getIsProvider()) {
                    return actionSpec.getRecommendation().getInfo().getReconfigure().getIsAddition()
                        ? String.format("Policy \"%s\" is Overutilized", policy.get().getPolicyInfo().getName())
                        : String.format("Policy \"%s\" is Underutilized", policy.get().getPolicyInfo().getName());
                }
                // constructing risk with policyName for move and reconfigure
                final Optional<String> commNames =
                    nonSegmentationCommoditiesToString(actionSpec.getRecommendation());
                final String seDisplayName =
                    entityDisplayNameGetter.apply(entity.getId());
                return String.format("\"%s\" doesn't comply with \"%s\"%s",
                    (seDisplayName != null) ? seDisplayName
                        : String.format("\"%s(%d)\"",
                            CommonDTO.EntityDTO.EntityType.forNumber(entity.getType()),
                              entity.getId()),
                    policy.get().getPolicyInfo().getDisplayName(),
                    commNames.isPresent() ? ", " + commNames.get() : "");
            }
        }
        return translateExplanation(actionSpec.getExplanation(), entityDisplayNameGetter);
    }

    @Nonnull
    private static String commodityDisplayName(@Nonnull final TopologyDTO.CommodityType commType,
                                               final boolean keepItShort) {
        if (keepItShort) {
            return UICommodityType.fromType(commType).apiStr();
        } else {
            return ActionDTOUtil.getCommodityDisplayName(commType);
        }
    }

    /**
     * Return comma seperated list of commodities to be reconfigured on the consumer.
     *
     * @param recommendation contains the entityId for the action.
     * @return String Return comma seperated list of commodities to be reconfigured on the consumer.
     */
    private static Optional<String> nonSegmentationCommoditiesToString(@Nonnull ActionDTO.Action recommendation) {
        if (!recommendation.getInfo().hasReconfigure()) {
            return Optional.empty();
        }
        // if its a reconfigure due to SEGMENTATION commodity (among other commodities),
        // we override the explanation generated by market eg., "Enable supplier to offer
        // requested resource(s) Segmentation, Network networkABC"
        // with "vmName doesn't comply with policyName, networkABC"
        if (recommendation.getExplanation().getReconfigure().getReconfigureCommodityCount() < 1) {
            return Optional.empty();
        }
        String commNames = ActionDTOUtil.getReasonCommodities(recommendation)
            .filter(comm -> !POLICY_COMMODITY_TYPES.contains(comm.getCommodityType().getType()))
            .map(ActionDTO.Explanation.ReasonCommodity::getCommodityType)
            .map(commType -> commodityDisplayName(commType, false))
            .collect(Collectors.joining(", "));
        if (commNames.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(commNames);
    }

    private static Optional<String> tryExtractPlacementPolicyId(@Nonnull ActionDTO.Action recommendation) {
        if (!recommendation.hasExplanation()) {
            return Optional.empty();
        }
        if (recommendation.getExplanation().hasMove()) {

            if (recommendation.getExplanation().getMove().getChangeProviderExplanationCount() < 1) {
                return Optional.empty();
            }

            List<ActionDTO.Explanation.ChangeProviderExplanation> explanations = recommendation.getExplanation()
                .getMove().getChangeProviderExplanationList();

            // We always go with the primary explanation if available
            Optional<ActionDTO.Explanation.ChangeProviderExplanation> primaryExp = explanations.stream()
                .filter(ActionDTO.Explanation.ChangeProviderExplanation::getIsPrimaryChangeProviderExplanation).findFirst();
            final ActionDTO.Explanation.ChangeProviderExplanation explanation = primaryExp.orElse(explanations.get(0));

            if (!explanation.hasCompliance()) {
                return Optional.empty();
            }
            if (explanation.getCompliance().getMissingCommoditiesCount() < 1) {
                return Optional.empty();
            }
            if (explanation.getCompliance().getMissingCommodities(0).getCommodityType()
                .getType() != CommonDTO.CommodityDTO.CommodityType.SEGMENTATION_VALUE) {
                return Optional.empty();
            }
            return Optional.of(explanation.getCompliance().getMissingCommodities(0).getCommodityType()
                .getKey());
        } else if (recommendation.getExplanation().hasReconfigure()) {
            if (recommendation.getExplanation().getReconfigure().getReconfigureCommodityCount() < 1) {
                return Optional.empty();
            }
            Optional<ActionDTO.Explanation.ReasonCommodity> reasonCommodity =
                recommendation.getExplanation().getReconfigure().getReconfigureCommodityList().stream()
                    .filter(comm -> POLICY_COMMODITY_TYPES
                        .contains(comm.getCommodityType().getType())).findFirst();
            if (!reasonCommodity.isPresent()) {
                return Optional.empty();
            }
            return Optional.of(reasonCommodity.get().getCommodityType().getKey());
        } else if (recommendation.getExplanation().hasProvision()) {
            if (!recommendation.getExplanation().getProvision().hasProvisionBySupplyExplanation()) {
                return Optional.empty();
            }
            ActionDTO.Explanation.ReasonCommodity reasonCommodity = recommendation.getExplanation()
                .getProvision().getProvisionBySupplyExplanation().getMostExpensiveCommodityInfo();
            if (reasonCommodity.getCommodityType().getType()
                != CommonDTO.CommodityDTO.CommodityType.SEGMENTATION_VALUE) {
                return Optional.empty();
            }
            return Optional.of(reasonCommodity.getCommodityType().getKey());
        } else {
            return Optional.empty();
        }
    }

    /**
     * Translates placeholders in the input string with values.
     *
     * <p>If the string doesn't start with the TRANSLATION_PREFIX, return the original string.
     * </p>
     *
     * <p>Otherwise, translate any translation fragments into text. Translation fragments have the
     * syntax:
     * {entity:(oid):(field-name):(default-value)}
     * </p>
     *
     * <p>Where "entity" is a static prefix, (oid) is the entity oid to look up, and (field-name)
     * is the name of the entity property to fetch. The entity property value will be substituted
     * into the string. If the entity is not found, or there is an error getting the property
     * value, the (default-value) will be used instead.</p>
     *
     * @param input the input string.
     * @param entityDisplayNameGetter the function that gets an entity id and returns the for it.
     * @return the translated explanation.
     */
    public static String translateExplanation(@Nonnull String input,
                               @Nonnull final Function<Long, String> entityDisplayNameGetter) {
        if (!input.startsWith(TRANSLATION_PREFIX)) {
            // most of the time, we probably won't need to translate anything.
            return input;
        }

        StringBuilder sb = new StringBuilder();
        int lastOffset = TRANSLATION_PREFIX.length(); // offset to start appending from
        // we do need some minor translation. fill in the blanks here.
        Matcher matcher = TRANSLATION_PATTERN.matcher(input);
        while (matcher.find()) {
            // append the part of the string between regions
            sb.append(input, lastOffset, matcher.start());
            lastOffset = matcher.end();
            // replace the pattern
            long oid = Long.valueOf(matcher.group(1));
            final Object fieldValue = getEntityField(oid, matcher.group(2), entityDisplayNameGetter);
            if (fieldValue != null) {
                sb.append(fieldValue);
            } else {
                // use the substitute/fallback value because there is no entity in topology
                sb.append(matcher.group(3));
            }
        }
        // add the remainder of the input string
        sb.append(input, lastOffset, input.length());
        return sb.toString();
    }

    @Nullable
    private static Object getEntityField(@Nonnull long oid, @Nonnull String property,
                                         @Nonnull final Function<Long, String> entityDisplayNameGetter) {
        Optional<ActionDTOUtil.EntityField> entityField =
            ActionDTOUtil.EntityField.findFieldBasedOnPlaceholder(property);

        if (entityField.isPresent()) {
            switch (entityField.get()) {
                case DISPLAY_NAME:
                    return entityDisplayNameGetter.apply(oid);
                default:
                    logger.error("Unknown entity field {}. Default value will be used.",
                        entityField.get());
                    return null;
            }
        } else {
            logger.error("Unknown placeholder {}. Default value will be used.", property);
            return null;
        }
    }
}

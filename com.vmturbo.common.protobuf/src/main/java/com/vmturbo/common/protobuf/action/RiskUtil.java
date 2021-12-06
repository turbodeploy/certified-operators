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

import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
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
     * @param policyDisplayNameGetter the function that gets the policy display with the input oid.
     *                                The null if the policy cannot be found.
     * @param entityDisplayNameGetter the function that gets the display name for an entity with
     *                                input oid. It returns null of such entity does not exists.
     * @return the risk description for the action.
     * @throws UnsupportedActionException If the type of the action is not supported.
     */
    public static String createRiskDescription(@Nonnull final ActionDTO.ActionSpec actionSpec,
           @Nonnull final Function<Long, String> policyDisplayNameGetter,
           @Nonnull final Function<Long, String> entityDisplayNameGetter) throws UnsupportedActionException {
        final Optional<Long> policyId = extractRiskPolicyId(actionSpec.getRecommendation());
        if (policyId.isPresent()) {
            final ActionDTO.ActionEntity entity =
                ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation());
            final long policyOid = policyId.get();

            final String policyDisplayName = policyDisplayNameGetter.apply(policyOid);
            if (policyDisplayName == null) {
                return actionSpec.getExplanation();
            }
            if (actionSpec.getRecommendation().getExplanation().hasProvision()) {
                return String.format("%s violation", policyDisplayName);
            } else {
                if (actionSpec.getRecommendation().getInfo().getReconfigure().getIsProvider()) {
                    return actionSpec.getRecommendation().getInfo().getReconfigure().getIsAddition()
                        ? String.format("Policy \"%s\" is Overutilized", policyDisplayName)
                        : String.format("Policy \"%s\" is Underutilized", policyDisplayName);
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
                        policyDisplayName,
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

    /**
     * Gets the id of policy that explains the risk.
     *
     * @param recommendation the action.
     * @return the policy id if the action has an associated policy and empty otherwise.
     */
    private static Optional<Long> extractRiskPolicyId(@Nonnull ActionDTO.Action recommendation) {
        if (recommendation.hasExplanation()) {
            return extractRiskReasonCommodityList(recommendation.getExplanation())
                .map(ReasonCommodity::getCommodityType)
                .map(c -> c.getKey())
                .flatMap(key -> extractPolicyIdFromKey(key, recommendation.getId()));
        } else {
            return Optional.empty();
        }
    }

    @Nonnull
    private static Optional<ReasonCommodity> extractRiskReasonCommodityList(
            @Nonnull Explanation explanation) {
        ReasonCommodity reasonCommodity = null;
        switch (explanation.getActionExplanationTypeCase()) {
            case MOVE:
                final MoveExplanation moveExplanation = explanation.getMove();
                if (moveExplanation.getChangeProviderExplanationCount() > 0) {
                    final List<ChangeProviderExplanation> explanations = moveExplanation
                            .getChangeProviderExplanationList();
                    // We always go with the primary explanation if available
                    final ChangeProviderExplanation changeProviderExplanation = explanations
                        .stream()
                        .filter(ChangeProviderExplanation::getIsPrimaryChangeProviderExplanation)
                        .findFirst()
                        .orElse(explanations.get(0));
                    if (changeProviderExplanation.hasCompliance()
                        && changeProviderExplanation.getCompliance().getMissingCommoditiesCount() > 0
                        && changeProviderExplanation.getCompliance().getMissingCommodities(0)
                            .getCommodityType().getType() == CommodityType.SEGMENTATION_VALUE) {
                        reasonCommodity = changeProviderExplanation.getCompliance()
                                .getMissingCommodities(0);
                    }
                }
                break;
            case RECONFIGURE:
                reasonCommodity = explanation.getReconfigure().getReconfigureCommodityList()
                        .stream()
                        .filter(comm -> POLICY_COMMODITY_TYPES
                            .contains(comm.getCommodityType().getType()))
                        .findFirst()
                        .orElse(null);
                break;
            case PROVISION:
                if (explanation.getProvision().hasProvisionBySupplyExplanation()) {
                    final ReasonCommodity mostExpensiveCommodityInfo = explanation
                        .getProvision()
                        .getProvisionBySupplyExplanation()
                        .getMostExpensiveCommodityInfo();
                    if (mostExpensiveCommodityInfo.getCommodityType().getType()
                            == CommodityType.SEGMENTATION_VALUE) {
                        reasonCommodity = mostExpensiveCommodityInfo;
                    }
                }
                break;
            default:
        }
        return Optional.ofNullable(reasonCommodity);
    }

    /**
     * Gets the id of policies that caused this action.
     *
     * @param action the action for get the list of policies for.
     * @return the list of policies.
     */
    @Nonnull
    public static Set<Long> extractPolicyIds(@Nonnull final ActionDTO.Action action) {
        return ActionDTOUtil.getReasonCommodities(action)
                .filter(c -> RiskUtil.POLICY_COMMODITY_TYPES.contains(c.getCommodityType().getType()))
                .map(ReasonCommodity::getCommodityType)
                .filter(c -> c.hasKey())
                .map(c -> c.getKey())
                .map(key -> extractPolicyIdFromKey(key, action.getId()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());
    }

    @Nonnull
    private static Optional<Long> extractPolicyIdFromKey(@Nonnull String key,
                                                         @Nonnull Long actionId) {
        try {
            return Optional.of(Long.valueOf(key));
        } catch (NumberFormatException ex) {
            logger.warn("Unexpected value for policy id \"{}\" associated to action {}",
                    key, actionId);
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

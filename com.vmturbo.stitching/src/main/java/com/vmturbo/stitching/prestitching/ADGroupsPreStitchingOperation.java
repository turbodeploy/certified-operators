package com.vmturbo.stitching.prestitching;

import static com.vmturbo.stitching.utilities.MergeEntities.mergeEntity;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import org.apache.commons.lang.StringUtils;

import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.EntityFieldMergers;
import com.vmturbo.stitching.utilities.EntityFieldMergers.EntityFieldMerger;

/**
 * PreStitching operation to merge AD groups for Business Users.
 *
 *<p></p>
 * When horizon user has access to multiple sites, we discover AD groups that user is member of for
 * each site separately and populate entity property "relatedGroups" with that info.
 * Here we merge this property values from each site into a single list of groups.
 */
public class ADGroupsPreStitchingOperation extends SharedEntityDefaultPreStitchingOperation {

    private static final Gson GSON = new Gson();

    /**
     * Merge AD groups discovered by different Horizon targets.
     * If Business User has access to multiple sites, we discover AD groups the user is member of
     * separately for each site.
     * We need to merge them into one list.
     */
    private static final EntityFieldMerger<Collection<String>>
        AD_GROUPS_MERGER =
        EntityFieldMergers.merge(ADGroupsPreStitchingOperation::getADRelatedGroups,
            ADGroupsPreStitchingOperation::setADRelatedGroups)
            .withMethod((a, b) -> Sets.newHashSet(Iterables.concat(a, b)));

    /**
     * Constructor.
     *
     */
    public ADGroupsPreStitchingOperation() {
        super(stitchingScopeFactory ->
            stitchingScopeFactory.multiEntityTypesScope(ImmutableList.of(
                EntityType.DESKTOP_POOL,
                EntityType.BUSINESS_USER)));
    }

    @Override
    protected void mergeSharedEntities(@Nonnull final StitchingEntity source,
                                       @Nonnull final StitchingEntity destination,
                                       @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        resultBuilder.queueEntityMerger(mergeEntity(source)
            .onto(destination)
            .addFieldMerger(AD_GROUPS_MERGER));
    }

    private static Collection<String> getADRelatedGroups(EntityDTOOrBuilder entity) {
        final Set<String> allGroupsCombined = new HashSet<>();
        final List<String> groupValues = entity.getEntityPropertiesList().stream().filter(p ->
            SupplyChainConstants.RELATED_GROUPS.equals(p.getName()))
            .map(EntityProperty::getValue)
            .filter(StringUtils::isNotBlank)
            .collect(Collectors.toList());
        // each value is json array of AD groups
        groupValues.forEach(v -> allGroupsCombined.addAll(GSON.fromJson(v, HashSet.class)));

        return allGroupsCombined;
    }

    private static void setADRelatedGroups(EntityDTO.Builder builder,
                                           Collection<String> groups) {
        // first find and delete existing properties, if any
        final int propCount = builder.getEntityPropertiesCount();
        for (int i = propCount - 1; i >= 0; i--) {
            if (SupplyChainConstants.RELATED_GROUPS.equals(builder.getEntityProperties(i).getName())) {
                builder.removeEntityProperties(i);
            }
        }

        builder.addEntityProperties(EntityBuilders.entityProperty()
                .named(SupplyChainConstants.RELATED_GROUPS)
                .withValue(GSON.toJson(groups)).build());
    }

}

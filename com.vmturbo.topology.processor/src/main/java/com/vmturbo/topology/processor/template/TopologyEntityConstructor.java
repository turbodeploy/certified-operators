package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.COMMODITY_KEY_SEPARATOR;
import static com.vmturbo.commons.analysis.AnalysisUtil.DSPM_OR_DATASTORE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityView;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Responsible for creating a TopologyEntityDTO from TemplateDTO. And those
 * entities should be unplaced. And also it will try to keep all commodity
 * constrains from the original topology entity.
 */
public class TopologyEntityConstructor {

    protected static final String COMMODITY_KEY_PREFIX = "AddFromTemplate::";

    private static final Logger logger = LogManager.getLogger();

    /**
     * Action type for creating entities from templates.
     */
    public enum TemplateActionType {
        /**
         * Replace the entity with the new entity created from the template
         */
        REPLACE("Replacing"),
        /**
         * Add the new entity created from the template
         */
        CLONE("Cloning");

        private final String description;

        TemplateActionType(@Nonnull String description) {
            this.description = description;
        }

        /**
         * Get action description.
         *
         * @return description
         */
        @Nonnull
        public String getDescription() {
            return description;
        }
    }

    /**
     * Create topology entities from a template. It modifies the original entity
     * with the reference to the new one.
     *
     * @param template template
     * @param originalTopologyEntity original TopologyEntity
     * @param actionType action type
     * @param identityProvider identity provider
     * @param entityType entity type
     * @param nameSuffix suffix for the entity name
     * @return topology entities
     * @throws TopologyEntityConstructorException error creating topology
     *             entities
     */
    @Nonnull
    public TopologyEntityImpl generateTopologyEntityBuilder(@Nonnull Template template,
            @Nullable TopologyEntityImpl originalTopologyEntity,
            @Nonnull TemplateActionType actionType, @Nonnull IdentityProvider identityProvider,
            int entityType, @Nullable String nameSuffix) throws TopologyEntityConstructorException {
        TopologyEntityImpl result = new TopologyEntityImpl()
                .setEntityState(EntityState.POWERED_ON).setAnalysisSettings(new AnalysisSettingsImpl()
                        .setIsAvailableAsProvider(true).setShopTogether(true));

        long oid = identityProvider.generateTopologyId();
        result.setOid(oid);

        String displayName = template.getTemplateInfo().getName() + " - "
                + actionType.getDescription();

        if (originalTopologyEntity != null) {
            // Modify original topology entity.
            if (actionType == TemplateActionType.REPLACE) {
                originalTopologyEntity.getOrCreateEdit()
                        .getOrCreateReplaced().setReplacementId(oid);
            }

            displayName += " " + originalTopologyEntity.getDisplayName();
        }

        if (nameSuffix != null) {
            displayName += " " + nameSuffix;
        }

        result.setDisplayName(displayName);
        result.setEntityType(entityType);

        return result;
    }

    @Nonnull
    public static Double getTemplateValue(@Nonnull Map<String, String> templateMap,
            @Nonnull String fieldName) throws TopologyEntityConstructorException {
        String valueStr = templateMap.get(fieldName);

        if (valueStr == null) {
            throw new TopologyEntityConstructorException(
                    "The field '" + fieldName + "' is missing from the template");
        }

        try {
            return Double.parseDouble(valueStr);
        } catch (NumberFormatException e) {
            throw new TopologyEntityConstructorException("The field '" + fieldName
                    + "' value cannot be converted to double: " + valueStr);
        }
    }

    /**
     * Get all {@link CommoditiesBoughtFromProviderView} which list of commodity
     * bought are active and have keys. And in new
     * CommoditiesBoughtFromProviderView, it will only keep commodity constraints.
     *
     * @param entityDTO {@link TopologyEntityImpl}
     * @return a list of {@link CommoditiesBoughtFromProviderView}, if input
     *         entityDTO is null, it will return a empty list.
     */
    @Nonnull
    public static List<CommoditiesBoughtFromProviderView> getActiveCommoditiesWithKeysGroups(
            @Nullable TopologyEntityImpl entityDTO) {
        if (entityDTO == null) {
            return Collections.emptyList();
        }
        return entityDTO.getCommoditiesBoughtFromProvidersList().stream()
                .map(TopologyEntityConstructor::keepCommodityBoughtActiveWithKey)
                .flatMap(commoditiesBoughtFromProvider -> commoditiesBoughtFromProvider
                        .map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toList());
    }

    /**
     * return a new {@link CommoditiesBoughtFromProviderView} which only keeps
     * commodity constraints, if there is no commodity constraints, it will
     * return Optional empty.
     *
     * @param commoditiesBoughtFromProvider origianl
     *            {@link CommoditiesBoughtFromProviderView}
     * @return Optional of {@link CommoditiesBoughtFromProviderView}
     */
    @Nonnull
    private static Optional<CommoditiesBoughtFromProviderView> keepCommodityBoughtActiveWithKey(
            @Nonnull final CommoditiesBoughtFromProviderView commoditiesBoughtFromProvider) {
        final List<CommodityBoughtView> commodityBoughtViews = commoditiesBoughtFromProvider
                .getCommodityBoughtList().stream().filter(CommodityBoughtView::getActive)
                .filter(commodityBoughtDTO -> !commodityBoughtDTO.getCommodityType().getKey()
                        .isEmpty())
                .collect(Collectors.toList());
        return commodityBoughtViews.isEmpty() ? Optional.empty()
                : Optional.of(new CommoditiesBoughtFromProviderImpl(commoditiesBoughtFromProvider)
                    .clearCommodityBought()
                    .addAllCommodityBought(commodityBoughtViews));
    }

    /**
     * Get all {@link CommoditySoldView} which are active and have keys.
     *
     * @param entityDTO {@link TopologyEntityImpl}
     * @return set of {@link CommoditySoldView}
     */
    @Nonnull
    public static Set<CommoditySoldView> getCommoditySoldConstraint(
            @Nullable TopologyEntityImpl entityDTO) {
        if (entityDTO == null) {
            return Collections.emptySet();
        }
        return entityDTO.getCommoditySoldListList().stream()
                .filter(CommoditySoldView::getActive)
                .filter(commoditySoldDTO -> !commoditySoldDTO.getCommodityType().getKey().isEmpty())
                .collect(Collectors.toSet());
    }

    /**
     * Add commodity sold and commodity bought constraints to
     * topologyEntityBuilder.
     *
     * @param topologyEntityBuilder builder of {@link TopologyEntityImpl}.
     * @param commoditySoldConstraints a set of commodity sold constraints.
     * @param commodityBoughtConstraints a list of
     *            {@link CommoditiesBoughtFromProviderView} which only contains
     *            commodity bought constraints.
     */
    public static void addCommodityConstraints(
            @Nonnull final TopologyEntityImpl topologyEntityBuilder,
            @Nonnull final Set<CommoditySoldView> commoditySoldConstraints,
            @Nonnull final List<CommoditiesBoughtFromProviderView> commodityBoughtConstraints) {
        topologyEntityBuilder.addAllCommoditySoldList(commoditySoldConstraints);
        // use linkedList here, since we need to remove first element after
        // copied the first element.
        final ListMultimap<Integer, CommoditiesBoughtFromProviderView> commoditiesBoughtConstraintsMap = LinkedListMultimap
                .create();
        commodityBoughtConstraints.stream()
                .filter(CommoditiesBoughtFromProviderView::hasProviderEntityType)
                .forEach(commoditiesBought -> commoditiesBoughtConstraintsMap
                        .put(commoditiesBought.getProviderEntityType(), commoditiesBought));
        final List<CommoditiesBoughtFromProviderView> commoditiesBoughtWithConstraintsGroup = new ArrayList<>();
        topologyEntityBuilder.getCommoditiesBoughtFromProvidersList().stream()
                .forEach(commoditiesBoughtGroup -> addCommodityBoughtConstraints(
                        commoditiesBoughtGroup, commoditiesBoughtWithConstraintsGroup,
                        commoditiesBoughtConstraintsMap));
        topologyEntityBuilder.clearCommoditiesBoughtFromProviders()
                .addAllCommoditiesBoughtFromProviders(commoditiesBoughtWithConstraintsGroup);
    }

    /**
     * Based on provider entity type to match topology entity commodity bought
     * group with commodity bought group with constraints. If there are matched
     * provider entity type, it will copy commodity bought constraints to
     * matched commodity bought group.
     *
     * @param commoditiesBoughtGroup topology entity's
     *            {@link CommoditiesBoughtFromProviderView}.
     * @param commoditiesBoughtWithConstraintsGroup all
     *            {@link CommoditiesBoughtFromProviderView}. which contains
     *            commodity bought constraints.
     * @param commoditiesBoughtConstraintsMap contains all commodity bought
     *            constraints.
     */
    private static void addCommodityBoughtConstraints(
            @Nonnull final CommoditiesBoughtFromProviderView commoditiesBoughtGroup,
            @Nonnull final List<CommoditiesBoughtFromProviderView> commoditiesBoughtWithConstraintsGroup,
            @Nonnull final ListMultimap<Integer, CommoditiesBoughtFromProviderView> commoditiesBoughtConstraintsMap) {
        if (!commoditiesBoughtConstraintsMap
                .containsKey(commoditiesBoughtGroup.getProviderEntityType())
                || !commoditiesBoughtGroup.hasProviderEntityType()) {
            commoditiesBoughtWithConstraintsGroup.add(commoditiesBoughtGroup);
            return;
        }
        final List<CommoditiesBoughtFromProviderView> commoditiesBoughtFromProviders = commoditiesBoughtConstraintsMap
                .get(commoditiesBoughtGroup.getProviderEntityType());
        if (commoditiesBoughtFromProviders.isEmpty()) {
            commoditiesBoughtWithConstraintsGroup.add(commoditiesBoughtGroup);
            return;
        }
        final CommoditiesBoughtFromProviderView commoditiesBoughtConstraintsGroup = commoditiesBoughtFromProviders
                .remove(0);
        boolean isContainsBicliqueCommodity = containsBicliqueCommodity(
                commoditiesBoughtConstraintsGroup);
        final CommoditiesBoughtFromProviderImpl commoditiesBoughtWithConstraints = generateCommodityBoughtWithConstraints(
                commoditiesBoughtGroup.copy(), commoditiesBoughtConstraintsGroup);
        if (isContainsBicliqueCommodity) {
            commoditiesBoughtWithConstraints
                    .setProviderId(commoditiesBoughtConstraintsGroup.getProviderId());
        }
        commoditiesBoughtWithConstraintsGroup.add(commoditiesBoughtWithConstraints);
    }

    /**
     * Check if contains DSPM or DATASTORE commodity, if yes, we need to keep
     * the provider id in order to get correct biclique key in Market component.
     *
     * @param commoditiesBoughtGroup {@link CommoditiesBoughtFromProviderView}.
     * @return Boolean indicate if contains DSPM or DATASTORE commodity.
     */
    private static boolean containsBicliqueCommodity(
            @Nonnull final CommoditiesBoughtFromProviderView commoditiesBoughtGroup) {
        return commoditiesBoughtGroup.getCommodityBoughtList().stream()
                .anyMatch(commodityBoughtDTO -> DSPM_OR_DATASTORE
                        .contains(commodityBoughtDTO.getCommodityType().getType()));
    }

    /**
     * Copy commodity bought constraints from commoditiesBoughtConstraintsGroup
     * to commoditiesBoughtGroup. And if there are same commodity type in
     * commoditiesBoughtGroup, it will only copy keys.
     *
     * @param commoditiesBoughtGroup need to keep commodity bought constraints.
     * @param commoditiesBoughtConstraintsGroup contains commodity bought
     *            constraints.
     * @return {@link CommoditiesBoughtFromProviderImpl}
     */
    private static CommoditiesBoughtFromProviderImpl generateCommodityBoughtWithConstraints(
            @Nonnull final CommoditiesBoughtFromProviderImpl commoditiesBoughtGroup,
            @Nonnull final CommoditiesBoughtFromProviderView commoditiesBoughtConstraintsGroup) {
        final List<CommodityBoughtView> commodityBoughtDTOS = new ArrayList<>();
        final ListMultimap<Integer, CommodityBoughtView> commodityTypeToCommodityBoughtDTO = LinkedListMultimap
                .create();
        commoditiesBoughtConstraintsGroup.getCommodityBoughtList().stream()
                .forEach(commodityBoughtDTO -> commodityTypeToCommodityBoughtDTO
                        .put(commodityBoughtDTO.getCommodityType().getType(), commodityBoughtDTO));
        commoditiesBoughtGroup.getCommodityBoughtList().stream()
                .map(commodityBoughtDTO -> generateCommodityBoughtDTOWithKeys(
                        commodityBoughtDTO.copy(), commodityTypeToCommodityBoughtDTO))
                .map(CommodityBoughtImpl::new).forEach(commodityBoughtDTOS::add);
        commodityTypeToCommodityBoughtDTO.asMap().entrySet().stream().map(Entry::getValue)
                .flatMap(Collection::stream).forEach(commodityBoughtDTOS::add);
        return commoditiesBoughtGroup.clearCommodityBought()
                .addAllCommodityBought(commodityBoughtDTOS);
    }

    /**
     * If commodityTypeToCommodityBoughtDTO has same commodity type as
     * commodityBoughtDTO, it will only copy over commodity key.
     *
     * @param commodityBoughtDTO {@link CommodityBoughtImpl}.
     * @param commodityTypeToCommodityBoughtDTO a list of Multimap which key is
     *            commodity type, value is commodity bought constraints.
     * @return {@link CommodityBoughtImpl}.
     */
    private static CommodityBoughtImpl generateCommodityBoughtDTOWithKeys(
            @Nonnull final CommodityBoughtImpl commodityBoughtDTO,
            @Nonnull final ListMultimap<Integer, CommodityBoughtView> commodityTypeToCommodityBoughtDTO) {
        final int commodityType = commodityBoughtDTO.getCommodityType().getType();
        if (!commodityTypeToCommodityBoughtDTO.containsKey(commodityType)) {
            return commodityBoughtDTO;
        }
        final List<CommodityBoughtView> commodityBoughtDTOS = commodityTypeToCommodityBoughtDTO
                .get(commodityType);
        if (commodityBoughtDTOS.isEmpty()) {
            return commodityBoughtDTO;
        }

        // remove related commodityBoughtDTO, since its key has been copied.
        final String commodityKey = commodityBoughtDTOS.remove(0).getCommodityType().getKey();
        final CommodityTypeView commodityTypeWithKey = commodityBoughtDTO.getCommodityType().copy()
                .setKey(commodityKey);
        return commodityBoughtDTO.clearCommodityType().setCommodityType(commodityTypeWithKey);
    }

    /**
     * Filter input template resources and only keep resources which category
     * name matched with input name parameter.
     *
     * @param template {@link Template} used to get all TemplateResource.
     * @param name {@link ResourcesCategoryName}.
     * @return a list of {@link TemplateResource} which category is equal to
     *         input name.
     */
    public static List<TemplateResource> getTemplateResources(@Nonnull Template template,
            @Nonnull ResourcesCategoryName name) {

        return template.getTemplateInfo().getResourcesList().stream()
                .filter(resource -> resource.getCategory().getName().equals(name))
                .collect(Collectors.toList());
    }

    /**
     * Get all template fields from template resources and generate a mapping
     * from template field name to template field value.
     *
     * @param templateResources list of {@link TemplateResource}.
     * @return A Map which key is template field name and value is template
     *         field value.
     */
    protected static Map<String, String> createFieldNameValueMap(
            @Nonnull final List<TemplateResource> templateResources) {
        final List<TemplateField> fields = getTemplateField(templateResources);
        // Since for one templateSpec, its field name is unique. There should be
        // no conflicts.
        return fields.stream()
                .collect(Collectors.toMap(TemplateField::getName, TemplateField::getValue));
    }

    /**
     * Get all template fields from template resources and generate a mapping
     * from template field name to template field value.
     *
     * @param template template
     * @param categoryName category name
     * @return A Map which key is template field name and value is template
     *         field value.
     */
    public static Map<String, String> createFieldNameValueMap(@Nonnull Template template,
            @Nonnull ResourcesCategoryName categoryName) {
        return createFieldNameValueMap(getTemplateResources(template, categoryName));
    }

    /**
     * Get all template fields from template resources.
     *
     * @param templateResources list of {@link TemplateResource}.
     * @return list of {@link TemplateField}.
     */
    private static List<TemplateField> getTemplateField(
            @Nonnull final List<TemplateResource> templateResources) {
        return templateResources.stream().map(TemplateResource::getFieldsList).flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Update accesses relationships that point to the original entity to instead to point to the replacement
     * entity. As an example: When replacing a host (Physical Machine), the host contains Datastore commodities
     * with Accesses relationships pointing to Storages. These Storages in turn contain DSPM commodities with
     * Accesses relationships that in turn point back to the host. When replacing the host, the Accesses
     * in the DSPM on the storages will point to the replacement host, and also the old host. As a result,
     * when the market attempts to create bicliques containing the hosts and storages, the replacement PM will
     * be in the biclique it belongs in and VMs will be able to move to the replacement host.
     *
     * <p>This method, in the example above, will add an additional equivalent commodity sold that Accesses
     * the replacement entity with a different key.
     *
     * @param originalEntity The entity being replaced.
     * @param replacementEntity The entity doing the replacement.
     * @param commoditySoldConstraints The constraint commodities that may contain accesses relationships that
     *                                 must be updated.
     * @param topology The topology map from OID -> TopologyEntity.Builder. When performing a replace,
     *                 entities related to the entity being replaced may be updated to fix up relationships
     *                 to point to the new entity along with the old entity.
     * @throws TopologyEntityConstructorException error processing access commodity
     */
    public static void updateRelatedEntityAccesses(
            @Nonnull TopologyEntityImpl originalEntity,
            @Nonnull TopologyEntityImpl replacementEntity,
            @Nonnull Collection<CommoditySoldView> commoditySoldConstraints,
            @Nonnull Map<Long, TopologyEntity.Builder> topology)
            throws TopologyEntityConstructorException {

        for (CommoditySoldView commoditySoldConstraint : commoditySoldConstraints) {
            if (!commoditySoldConstraint.hasAccesses()) {
                continue;
            }

            TopologyEntity.Builder entity = topology.get(commoditySoldConstraint.getAccesses());

            if (entity == null) {
                continue;
            }

            TopologyEntityImpl relatedEntity = entity.getTopologyEntityImpl();
            addAccess(originalEntity, replacementEntity, relatedEntity);
        }
    }

    /**
     * Add the access commodity to the related entity.
     *
     * @param originalEntity original entity
     * @param replacementEntity replacement entity
     * @param relatedEntity related entity
     * @throws TopologyEntityConstructorException error adding commodity
     */
    public static void addAccess(TopologyEntityImpl originalEntity,
            TopologyEntityImpl replacementEntity, TopologyEntityImpl relatedEntity)
            throws TopologyEntityConstructorException {
        CommoditySoldView commodityAccessing = createRelatedEntityAccesses(originalEntity,
                replacementEntity, relatedEntity);

        // Do not add the same commodity more then once
        if (commodityAccessing == null || hasCommodity(relatedEntity, commodityAccessing)) {
            return;
        }

        relatedEntity.addCommoditySoldList(commodityAccessing);
    }


    /**
     * Check if the entity has the access commodity.
     *
     * @param entity entity
     * @param comm access commodity
     * @return true, if has commodity
     */
    public static boolean hasCommodity(@Nonnull TopologyEntityView entity,
            @Nonnull CommoditySoldView comm) {
        return entity.getCommoditySoldListList().stream()
                .anyMatch(c -> c.getCommodityType().getType() == comm.getCommodityType().getType()
                        && c.getCommodityType().getKey().equals(comm.getCommodityType().getKey()));
    }

    /**
     * Create accesses relationship that point to the original entity to instead to point to the replacement
     * entity. As an example: When replacing a host (Physical Machine), the host contains Datastore commodities
     * with Accesses relationships pointing to Storages. These Storages in turn contain DSPM commodities with
     * Accesses relationships that in turn point back to the host. When replacing the host, the Accesses
     * in the DSPM on the storages will point to the replacement host, and also the old host. As a result,
     * when the market attempts to create bicliques containing the hosts and storages, the replacement PM will
     * be in the biclique it belongs in and VMs will be able to move to the replacement host.
     *
     * <p>This method, in the example above, will add an additional equivalent commodity sold that Accesses
     * the replacement entity with a different key.
     *
     * @param relatedEntity related entity
     * @param originalEntity The entity being replaced
     * @param replacementEntity The entity doing the replacement
     * @return access commodity
     * @throws TopologyEntityConstructorException error processing access commodity
     */
    @Nullable
    private static CommoditySoldView createRelatedEntityAccesses(
            @Nonnull TopologyEntityImpl originalEntity,
            @Nonnull TopologyEntityImpl replacementEntity,
            @Nonnull TopologyEntityImpl relatedEntity)
            throws TopologyEntityConstructorException {
        List<CommoditySoldImpl> commoditiesAccessingOriginals = relatedEntity
                .getCommoditySoldListImplList().stream()
                .filter(CommoditySoldView::hasAccesses)
                .filter(relatedEntityCommodity -> relatedEntityCommodity
                        .getAccesses() == originalEntity.getOid())
                .collect(Collectors.toList());

        if (commoditiesAccessingOriginals.isEmpty()) {
            logger.warn("Entity '{}' does not have access commodities related to '{}'",
                        originalEntity.getDisplayName(), relatedEntity.getDisplayName());
            return null;
        }

        if (commoditiesAccessingOriginals.size() > 1) {
            throw new TopologyEntityConstructorException("The accesses commodity for "
                    + originalEntity.getDisplayName() + " is not unique for the related entity "
                    + relatedEntity.getDisplayName());
        }

        CommoditySoldImpl commodityAccessingOriginal = commoditiesAccessingOriginals.get(0);

        // In addition to accessing the original, the related entity should also be able to
        // access the replacement. Keep the relation to the original as well because the original
        // remains in the topology until scoping happens in the market component.
        CommoditySoldImpl commodityAccessing = commodityAccessingOriginal.copy();
        setKeyAndAccess(commodityAccessing, replacementEntity.getOid(), null);

        return commodityAccessing;
    }

    /**
     * Create an access commodity.
     *
     * @param commType commodity type
     * @param oid related entity OID. It's used to set the Accesses field for
     *            DATASTORE and DSPM_ACCESS commodities.
     * @param key commodity key. If not present, the key is auto-generated based
     *            on OID
     * @return access commodity
     */
    public static CommoditySoldImpl createAccessCommodity(
            @Nonnull CommodityDTO.CommodityType commType, long oid, @Nullable String key) {
        CommodityTypeImpl commTypeImpl = new CommodityTypeImpl()
                .setType(commType.getNumber());
        CommoditySoldImpl result = new CommoditySoldImpl()
                .setCommodityType(commTypeImpl).setIsResizeable(false).setIsThin(true)
                .setActive(true).setUsed(SDKConstants.ACCESS_COMMODITY_USED)
                .setCapacity(SDKConstants.ACCESS_COMMODITY_CAPACITY);
        setKeyAndAccess(result, oid, key);

        return result;
    }

    /**
     * Set key and accesses for a new biclique commodity.
     *
     * <p>Different key with same Accesses will raise
     * java.lang.IllegalArgumentException: value already present at
     * com.vmturbo.market.topology.conversions.TopologyConverter.edge.
     * accessesByKey.computeIfAbsent(commSold.getCommodityType().getKey(), key
     * -> commSold.getAccesses()); Because accessesByKey is HashBiMap, where
     * both key and value should be unique.
     *
     * @param commodity commodity
     * @param accessesOid the entity being accessed
     * @param key commodity key. If key is null, it's auto-generated.
     */
    private static void setKeyAndAccess(@Nonnull CommoditySoldImpl commodity,
            long accessesOid, @Nullable String key) {
        CommodityDTO.CommodityType commType = convertCommodityType(commodity.getCommodityType());
        String newKey = key != null ? key
                : COMMODITY_KEY_PREFIX + commType.name() + COMMODITY_KEY_SEPARATOR
                        + accessesOid;
        commodity.getOrCreateCommodityType().setKey(newKey);

        if (commType == CommodityDTO.CommodityType.DATASTORE
                || commType == CommodityDTO.CommodityType.DSPM_ACCESS) {
            commodity.setAccesses(accessesOid);
        }
    }

    @Nonnull
    protected static CommodityDTO.CommodityType convertCommodityType(
            @Nonnull CommodityTypeView commodityType) {
        return CommodityDTO.CommodityType.internalGetValueMap()
                .findValueByNumber(commodityType.getType());
    }

    protected static CommodityBoughtView createCommodityBoughtView(int commodityType, double used) {
        return createCommodityBoughtView(commodityType, null, used);
    }

    /**
     * Create commodity bought DTO.
     *
     * @param commodityType commodity type
     * @param key key
     * @param used used value
     * @return commodity DTO
     */
    public static CommodityBoughtView createCommodityBoughtView(
        int commodityType, @Nullable String key, double used) {
        final CommodityTypeImpl commType = new CommodityTypeImpl()
                .setType(commodityType);

        if (key != null) {
            commType.setKey(key);
        }

        return new CommodityBoughtImpl().setUsed(used).setActive(true)
                .setCommodityType(commType);
    }

    /**
     * Create commodity sold DTO.
     *
     * @param commodityType commodity type
     * @param capacity capacity
     * @param isResizable is resizable
     * @return {@link CommoditySoldView}
     */
    public static CommoditySoldView createCommoditySoldDTO(int commodityType,
            @Nullable Double capacity, boolean isResizable) {
        final CommoditySoldImpl commoditySold = new CommoditySoldImpl();
        commoditySold.setActive(true)
                .setCommodityType(new CommodityTypeImpl().setType(commodityType));

        if (capacity != null) {
            commoditySold.setCapacity(capacity);
        }

        commoditySold.setIsResizeable(isResizable);
        return commoditySold;
    }

    /**
     * Create commodity sold DTO. {@link CommoditySoldView} from template is not
     * resizeable.
     *
     * @param commodityType commodity type
     * @param capacity capacity
     * @return {@link CommoditySoldView}
     */
    public static CommoditySoldView createCommoditySoldDTO(int commodityType,
            @Nullable Double capacity) {
        return createCommoditySoldDTO(commodityType, capacity, false);
    }

    /**
     * Create commodity sold DTO. {@link CommoditySoldView} from template is not
     * resizeable.
     *
     * @param commodityType commodity type.
     * @return {@link CommoditySoldView}
     */
    public static CommoditySoldView createCommoditySoldDTO(int commodityType) {
        return createCommoditySoldDTO(commodityType, null, false);
    }

    /**
     * Generate storage commodity sold and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntity.
     * @param fieldNameValueMap a Map which key is template field name and value
     *            is field value.
     * @param isResizable is commodity resizable
     * @throws TopologyEntityConstructorException error processing template
     */
    protected static void addStorageCommoditiesSold(
            @Nonnull final TopologyEntityImpl topologyEntityBuilder,
            @Nonnull Map<String, String> fieldNameValueMap, boolean isResizable)
            throws TopologyEntityConstructorException {
        Double diskSize = getTemplateValue(fieldNameValueMap, TemplateProtoUtil.STORAGE_DISK_SIZE);
        Double diskIops = getTemplateValue(fieldNameValueMap, TemplateProtoUtil.STORAGE_DISK_IOPS);

        CommoditySoldView storageAmoutCommodity = createCommoditySoldDTO(
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, diskSize, isResizable);
        CommoditySoldView storageAccessCommodity = createCommoditySoldDTO(
                CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE, diskIops, isResizable);

        // Since storage templates don't have a latency value, but VMs do buy a
        // latency commodity, a sold commodity is added.
        CommoditySoldView storageLatencyCommodity = createCommoditySoldDTO(
                CommodityDTO.CommodityType.STORAGE_LATENCY_VALUE,
                EntitySettingSpecs.LatencyCapacity.getNumericDefault(), isResizable);

        // Because we don't have access to settings at this time, we can't calculate capacities for
        // provisioned commodities. By leaving capacities unset, they will be set later in the
        // topology pipeline when settings are available by the
        // OverprovisionCapacityPostStitchingOperation.
        CommoditySoldView storageProvisionedCommodity = createCommoditySoldDTO(
                CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE, null, isResizable);

        topologyEntityBuilder.addCommoditySoldList(storageAccessCommodity)
                .addCommoditySoldList(storageAmoutCommodity)
                .addCommoditySoldList(storageLatencyCommodity)
                .addCommoditySoldList(storageProvisionedCommodity);
    }

    /**
     * Generate storage commodity bought and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntity.
     * @param providerId provider ID
     * @param fieldNameValueMap a Map which key is template field name and value
     *            is field value.
     * @throws TopologyEntityConstructorException error processing template
     */
    protected static void addStorageCommoditiesBought(
            @Nonnull TopologyEntityImpl topologyEntityBuilder,
            long providerId, @Nonnull Map<String, String> fieldNameValueMap)
            throws TopologyEntityConstructorException {

        Double diskSize = getTemplateValue(fieldNameValueMap, TemplateProtoUtil.STORAGE_DISK_SIZE);
        Double diskIops = getTemplateValue(fieldNameValueMap, TemplateProtoUtil.STORAGE_DISK_IOPS);

        CommodityBoughtView storageAmoutCommodity = createCommodityBoughtView(
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, diskSize);
        CommodityBoughtView storageAccessCommodity = createCommodityBoughtView(
                CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE, diskIops);

        CommodityBoughtView storageLatencyCommodity =
                createCommodityBoughtView(CommodityDTO.CommodityType.STORAGE_LATENCY_VALUE, 0);
        CommodityBoughtView storageProvisionedCommodity =
                createCommodityBoughtView(CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE,
                        diskSize);

        CommoditiesBoughtFromProviderImpl commoditiesBoughtGroup = new CommoditiesBoughtFromProviderImpl()
                .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setProviderId(providerId);

        commoditiesBoughtGroup.addCommodityBought(storageAmoutCommodity)
                .addCommodityBought(storageAccessCommodity)
                .addCommodityBought(storageLatencyCommodity)
                .addCommodityBought(storageProvisionedCommodity);

        topologyEntityBuilder.addCommoditiesBoughtFromProviders(commoditiesBoughtGroup);
    }
}

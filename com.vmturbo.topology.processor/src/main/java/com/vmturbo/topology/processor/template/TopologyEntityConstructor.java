package com.vmturbo.topology.processor.template;

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
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
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

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create topology entities from a template. It modifies the original entity
     * with the reference to the new one.
     *
     * @param template template
     * @param originalTopologyEntity original TopologyEntity
     * @param isReplaced is replaced
     * @param identityProvider identity provider
     * @param entityType entity type
     * @return topology entities
     * @throws TopologyEntityConstructorException error creating topology
     *             entities
     */
    @Nonnull
    public TopologyEntityDTO.Builder generateTopologyEntityBuilder(@Nonnull Template template,
            @Nullable TopologyEntityDTO.Builder originalTopologyEntity, boolean isReplaced,
            @Nonnull IdentityProvider identityProvider, int entityType)
            throws TopologyEntityConstructorException {
        TopologyEntityDTO.Builder result = TopologyEntityDTO.newBuilder()
                .setEntityState(EntityState.POWERED_ON).setAnalysisSettings(AnalysisSettings
                        .newBuilder().setIsAvailableAsProvider(true).setShopTogether(true));

        long oid = identityProvider.generateTopologyId();
        result.setOid(oid);

        String actionName = isReplaced ? " - Replacing " : " - Cloning ";
        String displayName = template.getTemplateInfo().getName() + actionName;

        if (originalTopologyEntity != null) {
            // Modify original topology entity.
            if (isReplaced) {
                originalTopologyEntity.getEditBuilder()
                        .getReplacedBuilder().setReplacementId(oid);
            }

            displayName += originalTopologyEntity.getDisplayName();
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
     * Get all {@link CommoditiesBoughtFromProvider} which list of commodity
     * bought are active and have keys. And in new
     * CommoditiesBoughtFromProvider, it will only keep commodity constraints.
     *
     * @param entityDTO {@link TopologyEntityDTO}
     * @return a list of {@link CommoditiesBoughtFromProvider}, if input
     *         entityDTO is null, it will return a empty list.
     */
    @Nonnull
    public static List<CommoditiesBoughtFromProvider> getActiveCommoditiesWithKeysGroups(
            @Nullable TopologyEntityDTO.Builder entityDTO) {
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
     * return a new {@link CommoditiesBoughtFromProvider} which only keeps
     * commodity constraints, if there is no commodity constraints, it will
     * return Optional empty.
     *
     * @param commoditiesBoughtFromProvider origianl
     *            {@link CommoditiesBoughtFromProvider}
     * @return Optional of {@link CommoditiesBoughtFromProvider}
     */
    @Nonnull
    private static Optional<CommoditiesBoughtFromProvider> keepCommodityBoughtActiveWithKey(
            @Nonnull final CommoditiesBoughtFromProvider commoditiesBoughtFromProvider) {
        final List<CommodityBoughtDTO> commodityBoughtDTOS = commoditiesBoughtFromProvider
                .getCommodityBoughtList().stream().filter(CommodityBoughtDTO::getActive)
                .filter(commodityBoughtDTO -> !commodityBoughtDTO.getCommodityType().getKey()
                        .isEmpty())
                .collect(Collectors.toList());
        return commodityBoughtDTOS.isEmpty() ? Optional.empty()
                : Optional.of(CommoditiesBoughtFromProvider
                        .newBuilder(commoditiesBoughtFromProvider).clearCommodityBought()
                        .addAllCommodityBought(commodityBoughtDTOS).build());
    }

    /**
     * Get all {@link CommoditySoldDTO} which are active and have keys.
     *
     * @param entityDTO {@link TopologyEntityDTO}
     * @return set of {@link CommoditySoldDTO}
     */
    @Nonnull
    public static Set<CommoditySoldDTO> getCommoditySoldConstraint(
            @Nullable TopologyEntityDTO.Builder entityDTO) {
        if (entityDTO == null) {
            return Collections.emptySet();
        }
        return entityDTO.getCommoditySoldListList().stream()
                .filter(CommoditySoldDTO::getActive)
                .filter(commoditySoldDTO -> !commoditySoldDTO.getCommodityType().getKey().isEmpty())
                .collect(Collectors.toSet());
    }

    /**
     * Add commodity sold and commodity bought constraints to
     * topologyEntityBuilder.
     *
     * @param topologyEntityBuilder builder of {@link TopologyEntityDTO}.
     * @param commoditySoldConstraints a set of commodity sold constraints.
     * @param commodityBoughtConstraints a list of
     *            {@link CommoditiesBoughtFromProvider} which only contains
     *            commodity bought constraints.
     */
    public static void addCommodityConstraints(
            @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
            @Nonnull final Set<CommoditySoldDTO> commoditySoldConstraints,
            @Nonnull final List<CommoditiesBoughtFromProvider> commodityBoughtConstraints) {
        topologyEntityBuilder.addAllCommoditySoldList(commoditySoldConstraints);
        // use linkedList here, since we need to remove first element after
        // copied the first element.
        final ListMultimap<Integer, CommoditiesBoughtFromProvider> commoditiesBoughtConstraintsMap = LinkedListMultimap
                .create();
        commodityBoughtConstraints.stream()
                .filter(CommoditiesBoughtFromProvider::hasProviderEntityType)
                .forEach(commoditiesBought -> commoditiesBoughtConstraintsMap
                        .put(commoditiesBought.getProviderEntityType(), commoditiesBought));
        final List<CommoditiesBoughtFromProvider> commoditiesBoughtWithConstraintsGroup = new ArrayList<>();
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
     *            {@link CommoditiesBoughtFromProvider}.
     * @param commoditiesBoughtWithConstraintsGroup all
     *            {@link CommoditiesBoughtFromProvider}. which contains
     *            commodity bought constraints.
     * @param commoditiesBoughtConstraintsMap contains all commodity bought
     *            constraints.
     */
    private static void addCommodityBoughtConstraints(
            @Nonnull final CommoditiesBoughtFromProvider commoditiesBoughtGroup,
            @Nonnull final List<CommoditiesBoughtFromProvider> commoditiesBoughtWithConstraintsGroup,
            @Nonnull final ListMultimap<Integer, CommoditiesBoughtFromProvider> commoditiesBoughtConstraintsMap) {
        if (!commoditiesBoughtConstraintsMap
                .containsKey(commoditiesBoughtGroup.getProviderEntityType())
                || !commoditiesBoughtGroup.hasProviderEntityType()) {
            commoditiesBoughtWithConstraintsGroup.add(commoditiesBoughtGroup);
            return;
        }
        final List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders = commoditiesBoughtConstraintsMap
                .get(commoditiesBoughtGroup.getProviderEntityType());
        if (commoditiesBoughtFromProviders.isEmpty()) {
            commoditiesBoughtWithConstraintsGroup.add(commoditiesBoughtGroup);
            return;
        }
        final CommoditiesBoughtFromProvider commoditiesBoughtConstraintsGroup = commoditiesBoughtFromProviders
                .remove(0);
        boolean isContainsBicliqueCommodity = containsBicliqueCommodity(
                commoditiesBoughtConstraintsGroup);
        final CommoditiesBoughtFromProvider.Builder commoditiesBoughtWithConstraints = generateCommodityBoughtWithConstraints(
                commoditiesBoughtGroup.toBuilder(), commoditiesBoughtConstraintsGroup);
        if (isContainsBicliqueCommodity) {
            commoditiesBoughtWithConstraints
                    .setProviderId(commoditiesBoughtConstraintsGroup.getProviderId());
        }
        commoditiesBoughtWithConstraintsGroup.add(commoditiesBoughtWithConstraints.build());
    }

    /**
     * Check if contains DSPM or DATASTORE commodity, if yes, we need to keep
     * the provider id in order to get correct biclique key in Market component.
     *
     * @param commoditiesBoughtGroup {@link CommoditiesBoughtFromProvider}.
     * @return Boolean indicate if contains DSPM or DATASTORE commodity.
     */
    private static boolean containsBicliqueCommodity(
            @Nonnull final CommoditiesBoughtFromProvider commoditiesBoughtGroup) {
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
     * @return {@link CommoditiesBoughtFromProvider.Builder}
     */
    private static CommoditiesBoughtFromProvider.Builder generateCommodityBoughtWithConstraints(
            @Nonnull final CommoditiesBoughtFromProvider.Builder commoditiesBoughtGroup,
            @Nonnull final CommoditiesBoughtFromProvider commoditiesBoughtConstraintsGroup) {
        final List<CommodityBoughtDTO> commodityBoughtDTOS = new ArrayList<>();
        final ListMultimap<Integer, CommodityBoughtDTO> commodityTypeToCommodityBoughtDTO = LinkedListMultimap
                .create();
        commoditiesBoughtConstraintsGroup.getCommodityBoughtList().stream()
                .forEach(commodityBoughtDTO -> commodityTypeToCommodityBoughtDTO
                        .put(commodityBoughtDTO.getCommodityType().getType(), commodityBoughtDTO));
        commoditiesBoughtGroup.getCommodityBoughtList().stream()
                .map(commodityBoughtDTO -> generateCommodityBoughtDTOWithKeys(
                        commodityBoughtDTO.toBuilder(), commodityTypeToCommodityBoughtDTO))
                .map(Builder::build).forEach(commodityBoughtDTOS::add);
        commodityTypeToCommodityBoughtDTO.asMap().entrySet().stream().map(Entry::getValue)
                .flatMap(Collection::stream).forEach(commodityBoughtDTOS::add);
        return commoditiesBoughtGroup.clearCommodityBought()
                .addAllCommodityBought(commodityBoughtDTOS);
    }

    /**
     * If commodityTypeToCommodityBoughtDTO has same commodity type as
     * commodityBoughtDTO, it will only copy over commodity key.
     *
     * @param commodityBoughtDTO {@link CommodityBoughtDTO.Builder}.
     * @param commodityTypeToCommodityBoughtDTO a list of Multimap which key is
     *            commodity type, value is commodity bought constraints.
     * @return {@link CommodityBoughtDTO.Builder}.
     */
    private static CommodityBoughtDTO.Builder generateCommodityBoughtDTOWithKeys(
            @Nonnull final CommodityBoughtDTO.Builder commodityBoughtDTO,
            @Nonnull final ListMultimap<Integer, CommodityBoughtDTO> commodityTypeToCommodityBoughtDTO) {
        final int commodityType = commodityBoughtDTO.getCommodityType().getType();
        if (!commodityTypeToCommodityBoughtDTO.containsKey(commodityType)) {
            return commodityBoughtDTO;
        }
        final List<CommodityBoughtDTO> commodityBoughtDTOS = commodityTypeToCommodityBoughtDTO
                .get(commodityType);
        if (commodityBoughtDTOS.isEmpty()) {
            return commodityBoughtDTO;
        }

        // remove related commodityBoughtDTO, since its key has been copied.
        final String commodityKey = commodityBoughtDTOS.remove(0).getCommodityType().getKey();
        final CommodityType commodityTypeWithKey = commodityBoughtDTO.getCommodityType().toBuilder()
                .setKey(commodityKey).build();
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
            @Nonnull TopologyEntityDTO.Builder originalEntity,
            @Nonnull TopologyEntityDTO replacementEntity,
            @Nonnull Collection<CommoditySoldDTO> commoditySoldConstraints,
            @Nullable Map<Long, TopologyEntity.Builder> topology)
            throws TopologyEntityConstructorException {

        if (topology == null) {
            return;
        }

        for (CommoditySoldDTO commoditySoldConstraint : commoditySoldConstraints) {
            if (!commoditySoldConstraint.hasAccesses()) {
                return;
            }

            TopologyEntity.Builder relatedEntity = topology
                    .get(commoditySoldConstraint.getAccesses());

            if (relatedEntity == null) {
                continue;
            }

            TopologyEntityDTO.Builder relatedEntityDTO = relatedEntity.getEntityBuilder();

            Optional<CommoditySoldDTO.Builder> commodityAccessing = createRelatedEntityAccesses(
                    relatedEntityDTO,
                    originalEntity, replacementEntity);

            if (!commodityAccessing.isPresent()) {
                return;
            }

            relatedEntityDTO.addCommoditySoldList(commodityAccessing.get());
        }
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
    @Nonnull
    public static Optional<CommoditySoldDTO.Builder> createRelatedEntityAccesses(
            @Nonnull TopologyEntityDTO.Builder relatedEntity,
            @Nonnull TopologyEntityDTO.Builder originalEntity,
            @Nonnull TopologyEntityDTO replacementEntity)
            throws TopologyEntityConstructorException {
        final List<CommoditySoldDTO.Builder> commoditiesAccessingOriginals = relatedEntity
                .getCommoditySoldListBuilderList().stream()
            .filter(CommoditySoldDTO.Builder::hasAccesses)
            .filter(relatedEntityCommodity -> relatedEntityCommodity.getAccesses() == originalEntity.getOid())
            .collect(Collectors.toList());

        if (commoditiesAccessingOriginals.isEmpty()) {
            logger.warn("Entity '{}' does not have access commodities related to '{}'",
                    originalEntity.getDisplayName(), relatedEntity.getDisplayName());
            return Optional.empty();
        }

        if (commoditiesAccessingOriginals.size() > 1) {
            throw new TopologyEntityConstructorException("The accesses commodity for "
                    + originalEntity.getDisplayName() + " is not unique for the related entity "
                    + relatedEntity.getDisplayName());
        }

        CommoditySoldDTO.Builder commodityAccessingOriginal = commoditiesAccessingOriginals.get(0);

        // In addition to accessing the original, the related entity should also be able to
        // access the replacement. Keep the relation to the original as well because the original
        // remains in the topology until scoping happens in the market component.
        CommoditySoldDTO.Builder commodityAccessing = commodityAccessingOriginal.clone();
        setKeyAndAccess(commodityAccessing, replacementEntity);

        return Optional.of(commodityAccessing);
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
     * @param replacementEntity the entity doing the replacement
     */
    public static void setKeyAndAccess(@Nonnull CommoditySoldDTO.Builder commodity,
            @Nonnull TopologyEntityDTO replacementEntity) {
        String newKey = replacementEntity.getDisplayName() + "::" + replacementEntity.getOid();
        commodity.getCommodityTypeBuilder().setKey(newKey);
        commodity.setAccesses(replacementEntity.getOid());
    }

    protected static CommodityBoughtDTO createCommodityBoughtDTO(int commodityType, double used) {
        return createCommodityBoughtDTO(commodityType, null, used);
    }

    protected static CommodityBoughtDTO createCommodityBoughtDTO(int commodityType,
            @Nullable String key, double used) {
        final CommodityType.Builder commType = CommodityType.newBuilder().setType(commodityType);

        if (key != null) {
            commType.setKey(key);
        }

        return CommodityBoughtDTO.newBuilder().setUsed(used).setActive(true)
                .setCommodityType(commType).build();
    }

    /**
     * Create commodity sold DTO.
     *
     * @param commodityType commodity type
     * @param capacity capacity
     * @param isResizable is resizable
     * @return {@link CommoditySoldDTO}
     */
    public static CommoditySoldDTO createCommoditySoldDTO(int commodityType,
            @Nullable Double capacity, boolean isResizable) {
        final CommoditySoldDTO.Builder builder = CommoditySoldDTO.newBuilder();
        builder.setActive(true).setCommodityType(CommodityType.newBuilder().setType(commodityType));

        if (capacity != null) {
            builder.setCapacity(capacity);
        }

        builder.setIsResizeable(isResizable);
        return builder.build();
    }

    /**
     * Create commodity sold DTO. {@link CommoditySoldDTO} from template is not
     * resizeable.
     *
     * @param commodityType commodity type
     * @param capacity capacity
     * @return {@link CommoditySoldDTO}
     */
    public static CommoditySoldDTO createCommoditySoldDTO(int commodityType,
            @Nullable Double capacity) {
        return createCommoditySoldDTO(commodityType, capacity, false);
    }

    /**
     * Create commodity sold DTO. {@link CommoditySoldDTO} from template is not
     * resizeable.
     *
     * @param commodityType commodity type.
     * @return {@link CommoditySoldDTO}
     */
    public static CommoditySoldDTO createCommoditySoldDTO(int commodityType) {
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
            @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
            @Nonnull Map<String, String> fieldNameValueMap, boolean isResizable)
            throws TopologyEntityConstructorException {
        Double diskSize = getTemplateValue(fieldNameValueMap, TemplateProtoUtil.STORAGE_DISK_SIZE);
        Double diskIops = getTemplateValue(fieldNameValueMap, TemplateProtoUtil.STORAGE_DISK_IOPS);

        CommoditySoldDTO storageAmoutCommodity = createCommoditySoldDTO(
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, diskSize, isResizable);
        CommoditySoldDTO storageAccessCommodity = createCommoditySoldDTO(
                CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE, diskIops, isResizable);

        // Since storage templates don't have a latency value, but VMs do buy a latency commodity,
        // a sold commodity is added. Capacity is left unset - StorageLatencyPostStitchingOperation
        // will set it to the default value from EntitySettingSpecs.LatencyCapacity.
        CommoditySoldDTO storageLatencyCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.STORAGE_LATENCY_VALUE);
        // Because we don't have access to settings at this time, we can't calculate capacities for
        // provisioned commodities. By leaving capacities unset, they will be set later in the
        // topology pipeline when settings are available by the
        // OverprovisionCapacityPostStitchingOperation.
        CommoditySoldDTO storageProvisionedCommodity = createCommoditySoldDTO(
                CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE, null, isResizable);

        topologyEntityBuilder
            .addCommoditySoldList(storageAccessCommodity)
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
            @Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
            long providerId, @Nonnull Map<String, String> fieldNameValueMap)
            throws TopologyEntityConstructorException {

        Double diskSize = getTemplateValue(fieldNameValueMap, TemplateProtoUtil.STORAGE_DISK_SIZE);
        Double diskIops = getTemplateValue(fieldNameValueMap, TemplateProtoUtil.STORAGE_DISK_IOPS);

        CommodityBoughtDTO storageAmoutCommodity = createCommodityBoughtDTO(
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, diskSize);
        CommodityBoughtDTO storageAccessCommodity = createCommodityBoughtDTO(
                CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE, diskIops);

        // Since storage templates don't have a latency value, but VMs do buy a latency commodity,
        // a sold commodity is added. Capacity is left unset - StorageLatencyPostStitchingOperation
        // will set it to the default value from EntitySettingSpecs.LatencyCapacity.
        CommodityBoughtDTO storageLatencyCommodity =
                createCommodityBoughtDTO(CommodityDTO.CommodityType.STORAGE_LATENCY_VALUE, 0.1);
        // Because we don't have access to settings at this time, we can't calculate capacities for
        // provisioned commodities. By leaving capacities unset, they will be set later in the
        // topology pipeline when settings are available by the
        // OverprovisionCapacityPostStitchingOperation.
        CommodityBoughtDTO storageProvisionedCommodity =
                createCommodityBoughtDTO(CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE,
                        diskSize);

        CommoditiesBoughtFromProvider.Builder commoditiesBoughtGroup = CommoditiesBoughtFromProvider
                .newBuilder().setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setProviderId(providerId);

        commoditiesBoughtGroup.addCommodityBought(storageAmoutCommodity)
                .addCommodityBought(storageAccessCommodity)
                .addCommodityBought(storageLatencyCommodity)
                .addCommodityBought(storageProvisionedCommodity);

        topologyEntityBuilder.addCommoditiesBoughtFromProviders(commoditiesBoughtGroup.build());
    }
}

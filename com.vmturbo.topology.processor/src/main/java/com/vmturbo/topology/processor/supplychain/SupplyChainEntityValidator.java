package com.vmturbo.topology.processor.supplychain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.SupplyChain.Provider;
import com.vmturbo.platform.common.dto.SupplyChain.Provider.ProviderType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateCommodity;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.CommBoughtProviderProp;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.supplychain.errors.MandatoryCommodityBagNotFoundException;
import com.vmturbo.topology.processor.supplychain.errors.MandatoryCommodityBoughtNotFoundException;
import com.vmturbo.topology.processor.supplychain.errors.MandatoryCommodityNotFoundException;
import com.vmturbo.topology.processor.supplychain.errors.ProviderCardinalityException;
import com.vmturbo.topology.processor.supplychain.errors.SupplyChainValidationException;

/**
 * This class contains methods to validate one entity against a set of supply chain templates.
 */
public class SupplyChainEntityValidator {
    private final Logger logger = LogManager.getLogger();

    /**
     * Entity to validate.
     */
    private final TopologyEntity entity;

    /**
     * Collection of templates to validate against.
     */
    private final Collection<TemplateDTO> templates;

    /**
     * List to add errors encountered during validation.
     */
    private final List<SupplyChainValidationException> validationExceptions;

    /**
     * Create a validator for a specific entity.
     *
     * @param templates collection of templates to validate against.
     * @param entity entity to validate.
     */
    private SupplyChainEntityValidator(
            @Nonnull Collection<TemplateDTO> templates,
            @Nonnull TopologyEntity entity) {
        this.validationExceptions = new ArrayList<>();
        this.templates = Objects.requireNonNull(templates);
        this.entity = Objects.requireNonNull(entity);
    }

    /**
     * Given an entity and a set of templates, use the {@link SupplyChainEntityValidator} class to validate
     * the entity against the set of templates.
     *
     * @param templates collection of templates to validate against.
     * @param entity entity to validate.
     * @return a list of errors found during validation.
     */
    public static List<SupplyChainValidationException> verify(
          @Nonnull Collection<TemplateDTO> templates, @Nonnull TopologyEntity entity) {
        return new SupplyChainEntityValidator(templates, entity).verify();
    }

    /**
     * Supply chain verification of the entity against the templates.
     *
     * @return a list of errors found during validation.
     */
    private List<SupplyChainValidationException> verify() {
        // verify entity against templates wrt. sold commodities
        verifySupplyChainCommoditiesSold();

        // verify entity against templates wrt. providers and bought commodities
        verifySupplyChainProvidersAndCommoditiesBought();

        // return collected errors
        return Collections.unmodifiableList(validationExceptions);
    }

    /**
     * Verifies that the entity is selling all the mandatory commodities specified by the templates.
     */
    private void verifySupplyChainCommoditiesSold() {
        logger.trace("Verifying sold commodities of entity {}", entity::getDisplayName);

        final TopologyEntityDTO.Builder entity = this.entity.getTopologyEntityDtoBuilder();

        // Create commodity type to commodity sold DTO map for better handling template match
        final Map<Integer, CommoditySoldDTO> commodityType2DTO = new HashMap<>();
        for (final CommoditySoldDTO commoditySold: entity.getCommoditySoldListList()) {
            commodityType2DTO.putIfAbsent(commoditySold.getCommodityType().getType(), commoditySold);
        }

        // get mandatory sold commodities from all the templates
        final List<TemplateCommodity> templateCommodityList =
            templates.stream().map(TemplateDTO::getCommoditySoldList).flatMap(List::stream).
            collect(Collectors.toList());

        for (final TemplateCommodity templateCommodity: templateCommodityList) {
            final int commodityTypeNumber = templateCommodity.getCommodityType().ordinal();
            logger.trace(
                "Entity {} must sell a commodity of type {}",
                entity::getDisplayName, () -> commodityTypeNumber);
            final CommoditySoldDTO commoditySoldDTO = commodityType2DTO.get(commodityTypeNumber);
            if (commoditySoldDTO == null) {
                validationExceptions.add(
                    new MandatoryCommodityNotFoundException(this.entity, templateCommodity, false));
                continue;
            }

            logger.trace(
                "Checking keys for entity {} and commodity {}",
                entity::getDisplayName, () -> commodityTypeNumber);
            final CommodityType commodityType = commoditySoldDTO.getCommodityType();
            final boolean commodityShouldHaveKey =
                templateCommodity.hasKey() && !Strings.isEmpty(templateCommodity.getKey());
            final boolean commodityHasKey =
                commodityType.hasKey() && !Strings.isEmpty(commodityType.getKey());
            if (commodityHasKey != commodityShouldHaveKey) {
                validationExceptions.add(
                    new MandatoryCommodityNotFoundException(
                        this.entity, templateCommodity, commodityShouldHaveKey));
            }
        }
    }

    /**
     * Verifies that the entity
     * - has all mandatory provider types and respects their cardinality bounds.
     * - buys all mandatory commodities per provider, as specified by the templates.
     */
    private void verifySupplyChainProvidersAndCommoditiesBought() {
        logger.trace("Verifying commodities bought by entity {}", entity::getDisplayName);

        // get all commodities bought by the entity under check and group them by provider
        final Map<Long, Set<CommoditiesBoughtFromProvider>> listsOfCommoditiesBoughtPerProvider =
            entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().stream().
            filter(CommoditiesBoughtFromProvider::hasProviderId).
            collect(Collectors.groupingBy(CommoditiesBoughtFromProvider::getProviderId, Collectors.toSet()));

        // every CommoditiesBoughtFromProvider object contains itself a list of commodities:
        // flatten the lists
        // now each provider id is mapped to the set of all commodities the entity buys from that provider
        final Map<Long, Set<CommodityBoughtDTO>> commoditiesBoughtPerProvider = new HashMap<>();
        listsOfCommoditiesBoughtPerProvider.forEach((providerId, listOfCommoditiesBought) ->
            commoditiesBoughtPerProvider.put(
                providerId,
                listOfCommoditiesBought.stream().map(CommoditiesBoughtFromProvider::getCommodityBoughtList).
                    flatMap(List::stream).collect(Collectors.toSet())));

        // collect the provider/bought commodities specifications (CommBoughtProviderProp objects)
        // from all the templates and check the entity against them
        templates.stream().map(TemplateDTO::getCommodityBoughtList).flatMap(Collection::stream).
        forEach((CommBoughtProviderProp providerSpecification) -> {
            logger.trace(
                "Checking commodities that the entity {} buys from providers of type {}",
                entity::getDisplayName, () -> providerSpecification.getKey().getTemplateClass());
            checkProviderAndBoughtCommoditiesAgainstSpecification(
                commoditiesBoughtPerProvider, providerSpecification);
        });
    }

    /**
     * Returns the set of providers, whose type matches that of a given provider specification.
     * Also checks cardinality constraints.  "Hosted by" relationships are treated as provider relationships
     * with cardinality either 0 or 1.
     *
     * @param providerSpecification a DTO containing a provider specification.
     * @return the set of all such providers of the entity.
     */
    @Nonnull
    private Set<TopologyEntity> findProvidersInEntity(
          @Nonnull CommBoughtProviderProp providerSpecification) {
        final Provider provider = Objects.requireNonNull(providerSpecification).getKey();
        final int providerCategoryId = provider.getTemplateClass().ordinal();

        // calculate cardinality bounds
        int cardinalityMin;
        final int cardinalityMax;
        if (provider.getProviderType() == ProviderType.HOSTING) {
            cardinalityMax = 1;
            cardinalityMin = 1;
        } else {
            cardinalityMax = provider.getCardinalityMax();
            cardinalityMin = provider.getCardinalityMin();
        }
        if (providerSpecification.getIsOptional()) {
            cardinalityMin = 0;
        }

        // get all matching providers
        final Set<TopologyEntity> allMatchingProviders =
            entity.getProviders().stream().filter(p -> p.getEntityType() == providerCategoryId).
            collect(Collectors.toSet());

        // check cardinality bounds
        int allMatchingProvidersCardinality = allMatchingProviders.size();
        if (allMatchingProvidersCardinality < cardinalityMin ||
            allMatchingProvidersCardinality > cardinalityMax) {
            validationExceptions.add(
                new ProviderCardinalityException(
                    entity, providerCategoryId, cardinalityMin, cardinalityMax,
                    allMatchingProvidersCardinality));
        }

        // return all matching providers
        return allMatchingProviders;
    }

    /**
     * Match a commodity bought against a template commodity.
     * The commodity types must be the same and the commodity should agree with the template on whether
     * there is a key
     *
     * @param boughtCommodity bought commodity to be matched.
     * @param templateCommodity template commodity to be matched against.
     * @return true iff match is successful
     */
    private boolean commodityMatch(
          @Nonnull CommodityBoughtDTO boughtCommodity, @Nonnull TemplateCommodity templateCommodity) {
        final CommodityType boughtCommodityType = Objects.requireNonNull(boughtCommodity).getCommodityType();

        // first check the commodity types
        if (boughtCommodityType.getType() != templateCommodity.getCommodityType().ordinal()) {
            return false;
        }

        // commodity types agree; now check if keys agree
        final boolean boughtCommodityHasKey =
            boughtCommodityType.hasKey() && !Strings.isEmpty(boughtCommodityType.getKey());
        final boolean boughtCommodityShouldHaveKey =
            Objects.requireNonNull(templateCommodity).hasKey() &&
            !Strings.isEmpty(templateCommodity.getKey());

        return boughtCommodityHasKey == boughtCommodityShouldHaveKey;
    }

    /**
     * Check a set of bought commodities against a bought commodity specification.
     * For any bought commodity in the template, there must be a matching commodity bought by the entity.
     *
     * @param commodityBoughtDTOs commodities bought by the entity.
     * @param providerSpecification specifies the commodities that should be bought by the entity.
     * @param matchingProvider the provider of the set of all {@code commodityBoughtDTOs}.
     */
    private void checkBoughtCommoditiesAgainstSpecification(
          @Nonnull Set<CommodityBoughtDTO> commodityBoughtDTOs,
          @Nonnull CommBoughtProviderProp providerSpecification,
          @Nonnull TopologyEntity matchingProvider) {
        Objects.requireNonNull(commodityBoughtDTOs);
        Objects.requireNonNull(matchingProvider);

        Objects.requireNonNull(providerSpecification).getValueList().forEach(templateCommodity -> {
            logger.trace(
                "Entity {} must buy from provider {} a commodity of type {}",
                entity::getDisplayName, matchingProvider::getDisplayName,
                () -> templateCommodity.getCommodityType().ordinal());
            if (commodityBoughtDTOs.stream().noneMatch(commodityBought ->
                    commodityMatch(commodityBought, templateCommodity))) {
                // found an instance of a missing commodity bought
                validationExceptions.add(new MandatoryCommodityBoughtNotFoundException(
                    entity, templateCommodity, matchingProvider));
            }
        });
    }

    /**
     * Checks entity against a provider specification.  A provider specification is a
     * {@link CommBoughtProviderProp} object that mandates:
     * - the type of the provider
     * - the cardinality bounds of this provider type
     * - what commodity types must be bought by this provider type
     *
     * @param commoditiesBoughtPerProvider map of all commodities bought by the entity, per provider.
     * @param providerSpecification provider specification.
     */
    private void checkProviderAndBoughtCommoditiesAgainstSpecification(
          @Nonnull Map<Long, Set<CommodityBoughtDTO>> commoditiesBoughtPerProvider,
          @Nonnull CommBoughtProviderProp providerSpecification) {
        Objects.requireNonNull(commoditiesBoughtPerProvider);

        // find all providers of the entity under check
        // whose type is described in providerSpecification
        // and check cardinality constraints
        final Set<TopologyEntity> matchingProviders =
            findProvidersInEntity(Objects.requireNonNull(providerSpecification));

        // for each matching provider, check the bought commodities against the specification
        matchingProviders.forEach(matchingProvider -> {
            final Set<CommodityBoughtDTO> commoditiesBag =
                commoditiesBoughtPerProvider.get(matchingProvider.getOid());
            if (commoditiesBag == null) {
                // let E be the entity under validation.
                // if the code reaches this point, we have the following situation:
                // - according to the topology graph, an entity P is a provider of E.
                // - according to the commodity DTOs, there is no bag of commodities sold by P to E.
                // this inconsistency is probably the result of a bug in the topology graph construction.
                validationExceptions.add(
                    new MandatoryCommodityBagNotFoundException(entity, matchingProvider));
            } else {
                checkBoughtCommoditiesAgainstSpecification(
                    commoditiesBag, providerSpecification, matchingProvider);
            }
        });
    }
}

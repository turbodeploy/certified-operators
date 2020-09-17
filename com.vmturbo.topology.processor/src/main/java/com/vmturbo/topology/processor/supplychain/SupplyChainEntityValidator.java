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
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.CommBoughtProviderOrSet;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.CommBoughtProviderProp;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.supplychain.errors.MandatoryCommodityBagNotFoundFailure;
import com.vmturbo.topology.processor.supplychain.errors.MandatoryCommodityBoughtNotFoundFailure;
import com.vmturbo.topology.processor.supplychain.errors.MandatoryCommodityNotFoundFailure;
import com.vmturbo.topology.processor.supplychain.errors.ProviderCardinalityFailure;
import com.vmturbo.topology.processor.supplychain.errors.SupplyChainValidationFailure;

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
     * In this map we group the bag of commodities bought by the entity under check, grouped by provider.
     * The key to the map is the provider id.  The map is used to facilitate access to the commodities
     * bought by the entities, which must be fetched by various different methods.
     */
    private final Map<Long, Collection<CommodityBoughtDTO>> commoditiesBoughtPerProvider = new HashMap<>();

    /**
     * Create a validator for a specific entity.
     *
     * @param templates collection of templates to validate against.
     * @param entity entity to validate.
     */
    private SupplyChainEntityValidator(
            @Nonnull Collection<TemplateDTO> templates,
            @Nonnull TopologyEntity entity) {
        this.templates = Objects.requireNonNull(templates);
        this.entity = Objects.requireNonNull(entity);

        // populate the map of entities bought
        // first get all commodities bought by the entity under check and group them by provider
        final Map<Long, Set<CommoditiesBoughtFromProvider>> listsOfCommoditiesBoughtPerProvider =
            entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().stream().
                filter(CommoditiesBoughtFromProvider::hasProviderId).
                collect(Collectors.groupingBy(CommoditiesBoughtFromProvider::getProviderId, Collectors.toSet()));

        // every CommoditiesBoughtFromProvider object contains itself a list of commodities:
        // flatten the lists
        // now each provider id is mapped to the set of all commodities the entity buys from that provider
        listsOfCommoditiesBoughtPerProvider.forEach((providerId, listOfCommoditiesBought) ->
            commoditiesBoughtPerProvider.put(
                providerId,
                listOfCommoditiesBought.
                    stream().
                    map(CommoditiesBoughtFromProvider::getCommodityBoughtList).
                    flatMap(List::stream).
                    collect(Collectors.toList())));
    }

    /**
     * Given an entity and a set of templates, use the {@link SupplyChainEntityValidator} class to validate
     * the entity against the set of templates.
     *
     * @param templates collection of templates to validate against.
     * @param entity entity to validate.
     * @return a list of errors found during validation.
     */
    public static List<SupplyChainValidationFailure> verify(
            @Nonnull Collection<TemplateDTO> templates,
            @Nonnull TopologyEntity entity) {
        return new SupplyChainEntityValidator(templates, entity).verify();
    }

    /**
     * Supply chain verification of the entity against the templates.
     *
     * @return a list of errors found during validation.
     */
    private List<SupplyChainValidationFailure> verify() {
        final List<SupplyChainValidationFailure> validationFailures = new ArrayList<>();

        // verify entity against templates wrt. sold commodities
        validationFailures.addAll(verifySupplyChainCommoditiesSold());

        // verify entity against templates wrt. providers and bought commodities
        validationFailures.addAll(verifySupplyChainProvidersAndCommoditiesBought());

        // verify entity against templates wrt. disjunctive specifications
        validationFailures.addAll(verifyAgainstDisjunctiveSpecifications());

        // return collected errors
        return Collections.unmodifiableList(validationFailures);
    }

    /**
     * Verifies that the entity is selling all the mandatory commodities specified by the templates.
     *
     * @return a list of errors found during validation.
     */
    private List<SupplyChainValidationFailure> verifySupplyChainCommoditiesSold() {
        logger.trace("Verifying sold commodities of entity {}", entity::getDisplayName);

        final List<SupplyChainValidationFailure> validationFailures = new ArrayList<>();

        final TopologyEntityDTO.Builder entity = this.entity.getTopologyEntityDtoBuilder();

        // Create commodity type to commodity sold DTO map for better handling template match
        final Map<Integer, CommoditySoldDTO> commodityType2DTO = new HashMap<>();
        for (final CommoditySoldDTO commoditySold: entity.getCommoditySoldListList()) {
            commodityType2DTO.putIfAbsent(commoditySold.getCommodityType().getType(), commoditySold);
        }

        // get mandatory sold commodities from all the templates
        final List<TemplateCommodity> templateCommodityList =
            templates.stream()
                    .map(TemplateDTO::getCommoditySoldList)
                    .flatMap(List::stream)
                    .filter(commodity -> !commodity.getOptional())
                    .collect(Collectors.toList());

        for (final TemplateCommodity templateCommodity: templateCommodityList) {
            final int commodityTypeNumber = templateCommodity.getCommodityType().ordinal();
            logger.trace(
                "Entity {} must sell a commodity of type {}",
                entity::getDisplayName, () -> commodityTypeNumber);
            final CommoditySoldDTO commoditySoldDTO = commodityType2DTO.get(commodityTypeNumber);
            if (commoditySoldDTO == null) {
                validationFailures.add(
                    new MandatoryCommodityNotFoundFailure(this.entity, templateCommodity, false));
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
                validationFailures.add(
                    new MandatoryCommodityNotFoundFailure(
                        this.entity, templateCommodity, commodityShouldHaveKey));
            }
        }

        return validationFailures;
    }

    /**
     * Verifies that the entity
     * - has all mandatory provider types and respects their cardinality bounds.
     * - buys all mandatory commodities per provider, as specified by the templates.
     *
     * @return a list of errors found during validation.
     */
    private List<SupplyChainValidationFailure> verifySupplyChainProvidersAndCommoditiesBought() {
        logger.trace("Verifying commodities bought by entity {}", entity::getDisplayName);

        final List<SupplyChainValidationFailure> validationFailures = new ArrayList<>();

        // collect the provider/bought commodities specifications (CommBoughtProviderProp objects)
        // from all the templates and check the entity against them
        templates.stream().map(TemplateDTO::getCommodityBoughtList).flatMap(Collection::stream).
        forEach(providerSpecification -> {
            logger.trace(
                "Checking commodities that the entity {} buys from providers of type {}",
                entity::getDisplayName, () -> providerSpecification.getKey().getTemplateClass());
            validationFailures.addAll(
                checkProviderAndBoughtCommoditiesAgainstSpecification(providerSpecification));
        });

        return validationFailures;
    }

    /**
     * Returns the set of providers, whose type matches that of a given provider specification.
     * Also checks cardinality constraints.  "Hosted by" relationships are treated as provider relationships
     * with cardinality either 0 or 1.
     *
     * @param providerSpecification a DTO containing a provider specification.
     * @param validationFailures list of errors on which to add a provider cardinality failure, if needed.
     * @return the set of all such providers of the entity.
     */
    @Nonnull
    private Set<TopologyEntity> findProvidersInEntity(
            @Nonnull CommBoughtProviderProp providerSpecification,
            @Nonnull List<SupplyChainValidationFailure> validationFailures) {
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
            validationFailures.add(
                new ProviderCardinalityFailure(
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
            @Nonnull CommodityBoughtDTO boughtCommodity,
            @Nonnull TemplateCommodity templateCommodity) {
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
     * @return a list of errors found during validation.
     */
    private List<SupplyChainValidationFailure> checkBoughtCommoditiesAgainstSpecification(
            @Nonnull Collection<CommodityBoughtDTO> commodityBoughtDTOs,
            @Nonnull CommBoughtProviderProp providerSpecification,
            @Nonnull TopologyEntity matchingProvider) {
        Objects.requireNonNull(commodityBoughtDTOs);
        Objects.requireNonNull(matchingProvider);

        final List<SupplyChainValidationFailure> validationFailures = new ArrayList<>();

        Objects.requireNonNull(providerSpecification).getValueList().forEach(templateCommodity -> {
            logger.trace(
                "Entity {} must buy from provider {} a commodity of type {}",
                entity::getDisplayName, matchingProvider::getDisplayName,
                () -> templateCommodity.getCommodityType().ordinal());
            if (!templateCommodity.getOptional() && commodityBoughtDTOs.stream().noneMatch(commodityBought ->
                    commodityMatch(commodityBought, templateCommodity))) {
                // found an instance of a missing commodity bought
                validationFailures.add(new MandatoryCommodityBoughtNotFoundFailure(
                    entity, templateCommodity, matchingProvider));
            }
        });

        return validationFailures;
    }

    /**
     * Checks entity against a provider specification.  A provider specification is a
     * {@link CommBoughtProviderProp} object that mandates:
     * - the type of the provider
     * - the cardinality bounds of this provider type
     * - what commodity types must be bought by this provider type
     *
     * @param providerSpecification provider specification.
     * @return a list of errors found during validation.
     */
    private List<SupplyChainValidationFailure> checkProviderAndBoughtCommoditiesAgainstSpecification(
            @Nonnull CommBoughtProviderProp providerSpecification) {
        Objects.requireNonNull(commoditiesBoughtPerProvider);

        final List<SupplyChainValidationFailure> validationFailures = new ArrayList<>();

        // find all providers of the entity under check
        // whose type is described in providerSpecification
        // and check cardinality constraints
        final Set<TopologyEntity> matchingProviders =
            findProvidersInEntity(Objects.requireNonNull(providerSpecification), validationFailures);

        // for each matching provider, check the bought commodities against the specification
        matchingProviders.forEach(matchingProvider -> {
            final Collection<CommodityBoughtDTO> commoditiesBag =
                commoditiesBoughtPerProvider.get(matchingProvider.getOid());
            if (commoditiesBag == null) {
                // let E be the entity under validation.
                // if the code reaches this point, we have the following situation:
                // - according to the topology graph, an entity P is a provider of E.
                // - according to the commodity DTOs, there is no bag of commodities sold by P to E.
                // this inconsistency is probably the result of a bug in the topology graph construction.
                validationFailures.add(
                    new MandatoryCommodityBagNotFoundFailure(entity, matchingProvider));
            } else {
                validationFailures.addAll(checkBoughtCommoditiesAgainstSpecification(
                    commoditiesBag, providerSpecification, matchingProvider));
            }
        });

        return validationFailures;
    }

    /**
     * Verifies that the entity respects all disjunctive specifications in the supply chain definition.
     * The disjunctive specifications are objects of type {@link CommBoughtProviderOrSet},
     * which sometimes appear in a template.  The entity must satisfy the disjunctive specification of
     * all the templates.
     *
     * A disjunctive specification contains a list of {@link CommBoughtProviderProp} specifications.
     * To satisfy the disjunctive specification, the entity must satisfy at least one of them.
     *
     * It is assumed that any disjunctive specification must have at least one disjunct.
     *
     * @return a list of errors found during validation.
     */
    private List<SupplyChainValidationFailure> verifyAgainstDisjunctiveSpecifications() {
        logger.trace("Verifying {} against disjunctive specifications", entity::getDisplayName);

        final List<SupplyChainValidationFailure> validationFailures = new ArrayList<>();

        // collect the disjunctive specifications (CommBoughtProviderOrSet objects)
        // from all the templates and check the entity against each one of them
        templates.stream().map(TemplateDTO::getCommBoughtOrSetList).flatMap(Collection::stream).
            forEach(disjunctiveSpecification ->
               validationFailures.addAll(
                    checkProviderAndBoughtCommoditiesAgainstDisjunctiveSpecification(
                        disjunctiveSpecification)));

        return validationFailures;
    }

    /**
     * Verifies the entity against one disjunctive specification.  The disjunctive specification consists of
     * a set of simple {@link CommBoughtProviderProp} specifications, called "disjuncts".  The entity must
     * satisfy at least one disjunct.  The entity will be (lazily) checked against disjuncts with the
     * {@link this#checkProviderAndBoughtCommoditiesAgainstSpecification} method.
     *
     * The errors from the individual checks will be collected.
     *
     * If one of the checks returns no errors, then the entity satisfies the corresponding disjunct and
     * therefore the disjunctive specification as a whole.  All errors from other checks will be discarded
     * and the method will return with an empty list of errors.
     *
     * If all the checks return errors, then the entity fails to satisfy the disjunctive specification.
     * Then, the list of all the errors from all the individual checks will be returned as a result.
     *
     * Example 1:
     * Suppose that the disjuncts are A, B, C.
     * Suppose that checking against A returns errors [E1, E2], checking against B returns error [E3], and
     * checking against C returns errors [E4, E5, E6].
     * Then the result of this method will be the list [E1, E2, E3, E4, E5, E6].
     *
     * Example 2:
     * Suppose again that the disjuncts are A, B, C.
     * Suppose that checking against A returns errors [E1, E2], checking against B returns no error, and
     * checking against C returns errors [E4, E5, E6].
     * Then the result of this method will be an empty list of errors, since the entity satisfies disjunct B.
     *
     * Note that checking happens lazily.  In particular, in example 2, if the order of checks is
     * A then B then C, then the check against C is not going to happen (the check against B will have
     * already succeeded).
     *
     * @param disjunctiveSpecification the disjunctive specification.
     * @return empty list, if the entity satisfies the disjunctive specification
     *         list of all errors for all disjuncts, if it doesn't
     */
    private List<SupplyChainValidationFailure>
        checkProviderAndBoughtCommoditiesAgainstDisjunctiveSpecification(
            CommBoughtProviderOrSet disjunctiveSpecification) {
        // collector of all errors
        final List<SupplyChainValidationFailure> validationFailures = new ArrayList<>();

        // check disjuncts until one disjunct is satisfied, or all disjuncts fail
        // variable success will be set to true if and only if one disjunct is satisfied
        // a list of all the errors found is accumulated to the list validationFailures
        final boolean success =
            disjunctiveSpecification.getCommBoughtList().stream().anyMatch(disjunct -> {
                // add errors from checking against this disjunct to the overall list of errors
                final List<SupplyChainValidationFailure> specificValidationFailures =
                        checkProviderAndBoughtCommoditiesAgainstSpecification(disjunct);
                validationFailures.addAll(specificValidationFailures);

                // return true if and only if there were no errors
                return specificValidationFailures.isEmpty();
            });

        if (success) {
            // entity satisfies disjunctive specification
            // no errors should be returned; all errors collected for failing disjuncts must be ignored
            return Collections.emptyList();
        } else {
            // entity does not satisfy disjunctive specification
            // errors collected from all the disjuncts should be returned
            return validationFailures;
        }
    }
}

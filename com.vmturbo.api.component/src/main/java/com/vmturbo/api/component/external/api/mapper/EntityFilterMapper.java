package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase.GroupUseCaseCriteria;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.search.Search.ClusterMembershipFilter;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ListFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ObjectFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition.VerticesCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingConditionOrBuilder;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * This class converts filter for filter entities in the API to the filter
 * used internally to filter groups.
 */
public class EntityFilterMapper {
    private static final Logger logger = LogManager.getLogger();

    // For normal criteria, user just need to provide a string (like a display name). But for
    // some criteria, UI allow user to choose from list of available options (like tags, state and
    // account id for now). These special criteria are hardcoded in UI side (see
    // "criteriaKeysWithOptions" in filter.registry.service.ts). UI will call another API
    // "/criteria/{elements}/options" to get a list of options for user to select from.
    // Note: these needs to be consistent with UI side and also the "elements" field defined in
    // "groupBuilderUseCases.json". If not, "/criteria/{elements}/options" will not be called
    // since UI gets all criteria from "groupBuilderUseCases.json" and check if the criteria
    // matches that inside "groupBuilderUseCases.json".
    public static final String ACCOUNT_OID = "BusinessAccount:oid:CONNECTED_TO:1";
    public static final String STATE = "state";
    public static final String NETWORKS = "networks";
    public static final String CONNECTED_NETWORKS_FIELD = "connectedNetworks";
    public static final String CONNECTED_STORAGE_TIER_FILTER_PATH = "StorageTier:oid:CONNECTED_FROM";
    public static final String VOLUME_ATTACHMENT_STATE_FILTER_PATH = SearchableProperties.VOLUME_REPO_DTO +
        "." + SearchableProperties.VOLUME_ATTACHMENT_STATE;

    private static final String CONSUMES = "CONSUMES";
    private static final String PRODUCES = "PRODUCES";
    private static final String CONNECTED_TO = "CONNECTED_TO";
    private static final String CONNECTED_FROM = "CONNECTED_FROM";

    public static final String EQUAL = "EQ";
    public static final String NOT_EQUAL = "NEQ";
    public static final String GREATER_THAN = "GT";
    public static final String LESS_THAN = "LT";
    public static final String GREATER_THAN_OR_EQUAL = "GTE";
    public static final String LESS_THAN_OR_EQUAL = "LTE";
    public static final String REGEX_MATCH = "RXEQ";
    public static final String REGEX_NO_MATCH = "RXNEQ";

    public static final String ELEMENTS_DELIMITER = ":";
    public static final String NESTED_FIELD_DELIMITER = "\\.";


    // set of supported traversal types, the string should be the same as groupBuilderUsecases.json
    private static final Set<String> TRAVERSAL_TYPES = ImmutableSet.of(
            CONSUMES, PRODUCES, CONNECTED_TO, CONNECTED_FROM);

    // map from the comparison symbol to the ComparisonOperator enum
    // the order matters, since when this is used for checking whether a string contains
    // ">" or ">=", we should check ">=" first
    private static final Map<String, ComparisonOperator>
            COMPARISON_SYMBOL_TO_COMPARISON_OPERATOR = initComparisonSymbolToOperatorMap();



    // map from the comparison string to the ComparisonOperator enum
    private static final Map<String, ComparisonOperator> COMPARISON_STRING_TO_COMPARISON_OPERATOR =
            ImmutableMap.<String, ComparisonOperator>builder()
                    .put(EQUAL, ComparisonOperator.EQ)
                    .put(NOT_EQUAL, ComparisonOperator.NE)
                    .put(GREATER_THAN, ComparisonOperator.GT)
                    .put(LESS_THAN, ComparisonOperator.LT)
                    .put(GREATER_THAN_OR_EQUAL, ComparisonOperator.GTE)
                    .put(LESS_THAN_OR_EQUAL, ComparisonOperator.LTE)
                    .build();

    private static final Map<String, Function<SearchFilterContext, List<SearchFilter>>>
                FILTER_TYPES_TO_PROCESSORS = initFilterTypesToProcessor();

    private final GroupUseCaseParser groupUseCaseParser;

    /**
     * Constructor for {@link EntityFilterMapper}.
     * @param groupUseCaseParser group use case parser.
     */
    public EntityFilterMapper(GroupUseCaseParser groupUseCaseParser) {
        this.groupUseCaseParser = groupUseCaseParser;
    }

    private static Map<String, Function<SearchFilterContext, List<SearchFilter>>>
                    initFilterTypesToProcessor() {
        final TraversalFilterProcessor traversalFilterProcessor = new TraversalFilterProcessor();
        final ImmutableMap.Builder<String, Function<SearchFilterContext, List<SearchFilter>>>
                filterTypesToProcessors = new ImmutableMap.Builder<>();
        filterTypesToProcessors.put(StringConstants.TAGS_ATTR, EntityFilterMapper::getTagProcessor);
        filterTypesToProcessors.put(StringConstants.CLUSTER, EntityFilterMapper::getClusterProcessor);
        filterTypesToProcessors.put(NETWORKS, EntityFilterMapper::getNetworkProcessor);
        filterTypesToProcessors.put(CONSUMES, traversalFilterProcessor);
        filterTypesToProcessors.put(PRODUCES, traversalFilterProcessor);
        filterTypesToProcessors.put(CONNECTED_FROM, traversalFilterProcessor);
        filterTypesToProcessors.put(CONNECTED_TO, traversalFilterProcessor);
        return filterTypesToProcessors.build();
    }

    private static StoppingCondition.Builder buildStoppingCondition(String currentToken) {
        return StoppingCondition.newBuilder().setStoppingPropertyFilter(
                        SearchProtoUtil.entityTypeFilter(currentToken));
    }

    private static Map<String, ComparisonOperator> initComparisonSymbolToOperatorMap() {
        Map<String, ComparisonOperator> symbolToOperator = new LinkedHashMap<>();
        symbolToOperator.put("!=", ComparisonOperator.NE);
        symbolToOperator.put(">=", ComparisonOperator.GTE);
        symbolToOperator.put("<=", ComparisonOperator.LTE);
        symbolToOperator.put("=", ComparisonOperator.EQ);
        symbolToOperator.put(">", ComparisonOperator.GT);
        symbolToOperator.put("<", ComparisonOperator.LT);
        return Collections.unmodifiableMap(symbolToOperator);
    }

    private static List<SearchFilter> getTagProcessor(SearchFilterContext context) {
      //TODO: the expression value coming from the UI is currently unsanitized.
        // It is assumed that the tag keys and values do not contain characters such as = and |.
        // This is reported as a JIRA issue OM-39039.
        final String operator = context.getFilter().getExpType();
        final boolean positiveMatch = isPositiveMatchingOperator(operator);
        if (isRegexOperator(operator)) {
            // regex match is required
            // break input into key and value
            final String[] keyval = context.getFilter().getExpVal().split("=");
            final String key = keyval[0];
            final String value = keyval[1];
            final PropertyFilter tagsFilter =
                    mapPropertyFilterForMultimapsRegex(
                            StringConstants.TAGS_ATTR, key, value, positiveMatch);
            return Collections.singletonList(SearchProtoUtil.searchFilterProperty(tagsFilter));
        } else {
            // exact match is required
            final PropertyFilter tagsFilter =
                    mapPropertyFilterForMultimapsExact(
                            StringConstants.TAGS_ATTR,
                            context.getFilter().getExpVal(),
                            positiveMatch);
            if (tagsFilter != null) {
                return Collections.singletonList(SearchProtoUtil.searchFilterProperty(tagsFilter));
            } else {
                return Collections.emptyList();
            }
        }
    }

    private static List<SearchFilter> getClusterProcessor(SearchFilterContext context) {
        ClusterMembershipFilter clusterFilter =
                        SearchProtoUtil.clusterFilter(
                            SearchProtoUtil.nameFilterRegex(
                                context.getFilter().getExpVal(),
                                context.getFilter().getExpType().equals(REGEX_MATCH),
                                context.getFilter().getCaseSensitive()));
                return Collections.singletonList(SearchProtoUtil.searchFilterCluster(clusterFilter));
    }

    private static List<SearchFilter> getNetworkProcessor(SearchFilterContext context) {
        final StringFilter stringFilter =
                        SearchProtoUtil.stringFilterRegex(
                            context.getFilter().getExpVal(),
                            context.getFilter().getExpType().equals(REGEX_MATCH),
                            context.getFilter().getCaseSensitive());
                    return Collections.singletonList(SearchProtoUtil.searchFilterProperty(
                            PropertyFilter.newBuilder()
                                .setPropertyName(CONNECTED_NETWORKS_FIELD)
                                .setListFilter(
                                    ListFilter.newBuilder()
                                        .setStringFilter(stringFilter))
                                .build()));
    }

    /**
     * Convert a list of  {@link FilterApiDTO} to a list of search parameters. Right now the
     * entity type sources have inconsistency between search service with group service requests.
     * For search service request, UI use class name field store entityType, but for creating
     * group request, UI use groupType field store entityType.
     *
     * @param criteriaList list of {@link FilterApiDTO}  received from the UI
     * @param entityType the name of entity type, such as VirtualMachine
     * @param nameQuery user specified search query for entity name. If it is not null, it will be
     *                  converted to a entity name filter.
     * @return list of search parameters to use for querying the repository
     */
    public List<SearchParameters> convertToSearchParameters(@Nonnull List<FilterApiDTO> criteriaList,
                                                            @Nonnull String entityType,
                                                            @Nullable String nameQuery) {
        Optional<List<FilterApiDTO>> filterApiDTOList =
                (criteriaList != null && !criteriaList.isEmpty())
                        ? Optional.of(criteriaList)
                        : Optional.empty();
        return filterApiDTOList
                .map(filterApiDTOs -> filterApiDTOs.stream()
                        .map(filterApiDTO -> filter2parameters(filterApiDTO, entityType, nameQuery))
                        .collect(Collectors.toList()))
                .orElse(ImmutableList.of(searchParametersForEmptyCriteria(entityType, nameQuery)));
    }

    /**
     * Create a map filter for the specified property name and
     * specified expression field coming from the UI.
     *
     * <p>The form of the value of the expression field is expected to be
     * "k=v1|k=v2|...", where k is the key and v1, v2, ... are the possible values.
     * If the expression does not conform to the expected format,
     * then a filter with empty key and values fields is generated.</p>
     *
     * <p>The filter created allows for multimap properties.  The values of such
     * properties are maps, in which multiple values may correspond to a single key.
     * For example key "user" -> ["peter" and "paul"].</p>
     *
     * @param propName property name to use for the search.
     * @param expField expression field coming from the UI.
     * @param positiveMatch if false, then negate the result of the filter.
     * @return the property filter; {@code null} when the expression cannot be parsed.
     */
    @Nullable
    public static PropertyFilter mapPropertyFilterForMultimapsExact(
            @Nonnull String propName,
            @Nonnull String expField,
            boolean positiveMatch) {
        final List<String> keyValuePairs = splitWithEscapes(expField, '|');
        String key = null;
        final List<String> values = new ArrayList<>();
        for (String kvp : keyValuePairs) {
            final List<String> kv = splitWithEscapes(kvp, '=');
            if (kv.size() != 2) {
                logger.error("Cannot parse {} as a key/value pair", kvp);
                return null;
            }
            if (key == null) {
                key = removeEscapes(kv.get(0));
            } else if (!key.equals(removeEscapes(kv.get(0)))) {
                logger.error("Map filter {} contains more than one key", expField);
                return null;
            }
            if (!kv.get(1).isEmpty()) {
                values.add(removeEscapes(kv.get(1)));
            } else {
                logger.error("Cannot parse {} as a key/value pair", kvp);
                return null;
            }
        }

        if (key == null) {
            logger.error("Cannot parse {} to generate a string filter", expField);
            return null;
        }

        final PropertyFilter propertyFilter = PropertyFilter.newBuilder()
            .setPropertyName(propName)
            .setMapFilter(MapFilter.newBuilder()
                .setKey(key)
                .addAllValues(values)
                .setPositiveMatch(positiveMatch)
                .build())
            .build();
        logger.debug("Property filter constructed: {}", propertyFilter);
        return propertyFilter;
    }

    /**
     * Create a map filter for the specified property name
     * and specified regex coming from the UI.
     *
     * <p>This filter should match values to the regex expression.</p>
     *
     * <p>The filter created allows for multimap properties.  The values of such
     * properties are maps, in which multiple values may correspond to a single key.
     * For example key "user" -> ["peter" and "paul"].</p>
     *
     * @param propName the property name to use for the search.
     * @param key the key to search for.
     * @param regex the regex to match values against.
     * @param positiveMatch if false, then negate the result of the filter.
     * @return the property filter.
     */
    @Nonnull
    public static PropertyFilter mapPropertyFilterForMultimapsRegex(
            @Nonnull String propName, @Nonnull String key,
            @Nonnull String regex, boolean positiveMatch) {
        final PropertyFilter propertyFilter = PropertyFilter.newBuilder()
            .setPropertyName(propName)
            .setMapFilter(MapFilter.newBuilder()
                // The key is not a regex, so remove backslashes.
                .setKey(key.replaceAll("\\\\", ""))
                .setRegex(SearchProtoUtil.makeFullRegex(regex))
                .setPositiveMatch(positiveMatch)
                .build())
            .build();
        logger.debug("Property filter constructed: {}", propertyFilter);

        return propertyFilter;
    }

    /**
     * Takes a string and removes all backslashes that are used to escape
     * characters.  For example \\a\b will be changed to \ab
     *
     * @param string string to handle.
     * @return the string without the backlashes that are used to escape characters
     */
    @Nonnull
    private static String removeEscapes(@Nonnull String string) {
        return string.replaceAll("\\\\(.)", "$1");
    }

    /**
     * Split a string into a list of strings, given the breaking character.
     * The breaking character may be escaped (with a backlash), in which case
     * it gets treated as a regular character. Backslashes are kept in the
     * resulting lists, except those that escape the breaking character.
     *
     * <p>Example: if the breaking character is '=' and the string to break is
     * "AA\==B\+", then the method will return a list of two strings: "AA="
     * and "B\+".</p>
     *
     * @param string the string to split.
     * @param breakingChar the character to break at.
     * @return the breaking character.
     */
    @Nonnull
    public static List<String> splitWithEscapes(@Nonnull String string, char breakingChar) {
        // create a regex that describes the breaking character
        // even if that character is a Java regex metacharacter
        final String breakingCharacterPattern = Pattern.quote(Character.toString(breakingChar));

        // create a pattern that describes the pieces of the string
        // when broken at the unescaped breaking character
        final Pattern stringPartsPattern =
                Pattern.compile("([^\\\\" + breakingCharacterPattern + "]|\\\\.)+");

        // find all matches and return them as a list
        final Matcher matcher = stringPartsPattern.matcher(string);
        final List<String> stringParts = new ArrayList<>();
        while (matcher.find()) {
            stringParts.add(matcher.group());
        }
        return stringParts;
    }

    /**
     * Generate search parameters for getting all entities under current entity type. It handles the edge case
     * when users create dynamic group, not specify any criteria rules, this dynamic group should
     * contains all entities under selected type.
     *
     * @param entityType entity type from UI
     * @param nameQuery user specified search query for entity name. If it is not null, it will be
     *                  converted to a entity name filter.
     * @return a search parameters object only contains starting filter with entity type
     */
    private SearchParameters searchParametersForEmptyCriteria(@Nonnull final String entityType,
                                                              @Nullable final String nameQuery) {
        PropertyFilter byType = SearchProtoUtil.entityTypeFilter(entityType);
        final SearchParameters.Builder searchParameters = SearchParameters.newBuilder()
                        .setStartingFilter(byType);
        if (!StringUtils.isEmpty(nameQuery)) {
            // For the query string, we want to use a "contains"-type query.
            searchParameters.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                            SearchProtoUtil.nameFilterRegex(".*" + nameQuery + ".*")));
        }
        return searchParameters.build();
    }

    /**
     * <p>Convert one filter DTO to search parameters. The byName filter must have a filter type. If
     * it also has an expVal then use it to filter the results.</p>
     *
     * <p>Examples of "elements" with 2 and 3  elements:</p>
     *
     * <p>1. <b>Storage:PRODUCES</b> - start with instances of Storage, traverse to PRODUCES and stop
     * when reaching instances of byNameClass<br>
     * 2. <b>PhysicalMachine:PRODUCES:1</b> - start with instances of PhysicalMachine, traverse to
     * PRODUCES and stop after one hop.</p>
     *
     * <p>TODO: if the filter language keeps getting expanded on, we might want to use an actual
     * grammar and parser. This implementation is pretty loose in regards to checks and assumes the
     * input set is controlled via groupBuilderUsescases.json. We are not checking all boundary
     * conditions. If this becomes necessary then we'll need to update this implementation.</p>
     *
     * @param filter a filter
     * @param entityType class name of the byName filter
     * @param nameQuery user specified search query for entity name. If it is not null, it will be
     *                  converted to a entity name filter.
     * @return parameters full search parameters
     */
    private SearchParameters filter2parameters(@Nonnull FilterApiDTO filter,
                                               @Nonnull String entityType,
                                               @Nullable String nameQuery) {
        GroupUseCaseCriteria useCase = groupUseCaseParser.getUseCasesByFilterType()
                        .get(filter.getFilterType());

        if (useCase == null) {
            throw new IllegalArgumentException("Not existing filter type provided: "
                                + filter.getFilterType());
        }
        // build the search parameters based on the filter criteria
        SearchParameters.Builder parametersBuilder = SearchParameters.newBuilder();

        final List<String> elements = Arrays.asList(useCase.getElements()
                        .split(ELEMENTS_DELIMITER));

        Iterator<String> iterator = elements.iterator();
        final String firstToken = iterator.next();
        if (UIEntityType.fromString(firstToken) != UIEntityType.UNKNOWN) {
            parametersBuilder.setStartingFilter(SearchProtoUtil.entityTypeFilter(firstToken));
        } else {
            parametersBuilder.setStartingFilter(SearchProtoUtil.entityTypeFilter(entityType));
            iterator = elements.iterator();
        }

        final ImmutableList.Builder<SearchFilter> searchFilters = new ImmutableList.Builder<>();
        while (iterator.hasNext()) {
            searchFilters.addAll(processToken(filter, entityType, iterator, useCase.getInputType(),
                            firstToken));
        }
        if (!StringUtils.isEmpty(nameQuery)) {
            searchFilters.add(SearchProtoUtil.searchFilterProperty(
                SearchProtoUtil.nameFilterRegex(".*" + nameQuery + ".*")));
        }
        parametersBuilder.addAllSearchFilter(searchFilters.build());
        parametersBuilder.setSourceFilterSpecs(toFilterSpecs(filter));
        return parametersBuilder.build();
    }

    /**
     * Check whether the token is a list token.
     * For example: "commoditySoldList[type=VMem,capacity]".
     *
     * @param token the token to check list for
     * @return true if the token is a list, otherwise false
     */
    private boolean isListToken(@Nonnull String token) {
        int left = token.indexOf('[');
        int right = token.lastIndexOf(']');
        return left != -1 && right != -1 && left < right;
    }

    @Nonnull
    private SearchParameters.FilterSpecs toFilterSpecs(FilterApiDTO filter) {
        return SearchParameters.FilterSpecs.newBuilder()
                        .setExpressionType(filter.getExpType())
                        .setExpressionValue(filter.getExpVal())
                        .setFilterType(filter.getFilterType())
                        .setIsCaseSensitive(filter.getCaseSensitive())
                        .build();
    }

    /**
     * Process the given tokens which comes from the elements in "groupBuilderUsecases.json" and
     * convert those criteria into list of SearchFilter which will be used by repository to fetch
     * matching entities.
     *
     * @param filter the FilterApiDTO provided by user in UI, which contains the criteria for this group
     * @param entityType the entity type of the group to create
     * @param iterator the Iterator containing all the tokens to process, for example:
     * "PRODUCES:1:VirtualMachine", first is PRODUCES, then 1, and then VirtualMachine
     * @param inputType the type of the input, such as "*" or "#"
     * @param firstToken the first token defined in the "groupBuilderUsecases.json", for example:
     * "PRODUCES:1:VirtualMachine", first is PRODUCES.
     * @return list of SearchFilters for given tokens
     */
    private List<SearchFilter> processToken(@Nonnull FilterApiDTO filter,
                                            @Nonnull String entityType,
                                            @Nonnull Iterator<String> iterator,
                                            @Nonnull String inputType,
                                            @Nonnull String firstToken) {
        final String currentToken = iterator.next();

        final SearchFilterContext filterContext = new SearchFilterContext(filter, iterator,
                entityType, currentToken, firstToken);
        final Function<SearchFilterContext, List<SearchFilter>> filterApiDtoProcessor =
                        FILTER_TYPES_TO_PROCESSORS.get(currentToken);

        if (filterApiDtoProcessor != null) {
            return filterApiDtoProcessor.apply(filterContext);
        } else if (UIEntityType.fromString(currentToken) != UIEntityType.UNKNOWN) {
            return ImmutableList.of(SearchProtoUtil.searchFilterProperty(
                    SearchProtoUtil.entityTypeFilter(currentToken)));
        } else {
            final PropertyFilter propertyFilter = isListToken(currentToken) ?
                    createPropertyFilterForListToken(currentToken, inputType, filter) :
                    createPropertyFilterForNormalToken(currentToken, inputType, filter);
            return ImmutableList.of(SearchProtoUtil.searchFilterProperty(propertyFilter));
        }
    }

    /**
     * Create PropertyFilter for a list token.
     * The list token starts with the name of the property which is a list, and then wrap filter
     * criteria with "[" and "]". Each criteria is a key value pair combined using "=". If the
     * criteria starts with "#", it means this value is numeric, otherwise it is a string. The last
     * criteria may be a special one which doesn't start with "#" or contains "=", it is just a
     * single property whose value and type are provided by UI.
     * For example: currentToken: "commoditySoldList[type=VMem,#used>0,capacity]". It means finds
     * entities whose VMem commodity's used is more than 0 and capacity meets the value defined in
     * FilterApiDTO.
     *
     * @param currentToken the token which contains nested fields
     * @param inputType the type of the input from UI, which can be "*" (string) or "#" (number)
     * @param filter the FilterApiDTO which contains values provided by user in UI
     * @return PropertyFilter
     */
    private PropertyFilter createPropertyFilterForListToken(@Nonnull String currentToken,
            @Nonnull String inputType, @Nonnull FilterApiDTO filter) {
        // list, for example: "commoditySoldList[type=VMem,#used>0,capacity]"
        int left = currentToken.indexOf('[');
        int right = currentToken.lastIndexOf(']');
        // name of the property which is a list, for example: commoditySoldList
        final String listFieldName = currentToken.substring(0, left);

        ListFilter.Builder listFilter = ListFilter.newBuilder();
        // there is no nested property inside list, the list is a list of strings or numbers
        // for example: targetIds[]
        if (left == right) {
            switch (inputType) {
                case "s":
                case "s|*":
                case "*|s":
                case "*":
                    // string matching
                    // the input types "s" and "*" represent exact string matching
                    // and regex matching respectively.  their combination allows for both.
                    // all cases are treated in the same way here (the distinction is only
                    // helpful to the UI side).  we can distinguish the cases by looking at
                    // the operator
                    final boolean positiveMatch = isPositiveMatchingOperator(filter.getExpType());
                    if (isRegexOperator(filter.getExpType())) {
                        listFilter.setStringFilter(
                            SearchProtoUtil.stringFilterRegex(
                                filter.getExpVal(),
                                positiveMatch,
                                false));
                    } else {
                        listFilter.setStringFilter(
                            SearchProtoUtil.stringFilterExact(
                                Arrays.stream(filter.getExpVal().split("\\|"))
                                        .collect(Collectors.toList()),
                                positiveMatch,
                                false));
                    }
                    break;
                case "#":
                    // numeric comparison
                    listFilter.setNumericFilter(SearchProtoUtil.numericFilter(
                        Long.valueOf(filter.getExpVal()),
                            COMPARISON_STRING_TO_COMPARISON_OPERATOR.get(filter.getExpType())));
                    break;
                default:
                    throw new UnsupportedOperationException("Input type: " + inputType +
                            " is not supported for ListFilter");
            }
        } else {
            ObjectFilter.Builder objectFilter = ObjectFilter.newBuilder();
            // for example: "type=VMem,#used>0,capacity"
            final String nestedListField = currentToken.substring(left + 1, right);
            for (String criteria : nestedListField.split(",")) {
                if (isListToken(criteria)) {
                    // create nested list filter recursively
                    objectFilter.addFilters(createPropertyFilterForListToken(criteria,
                            inputType, filter));
                } else if (criteria.startsWith("#")) {
                    // this is numeric, find the symbol (>) in "#used>0"
                    String symbol = null;
                    int indexOfSymbol = 0;
                    for (String sb : COMPARISON_SYMBOL_TO_COMPARISON_OPERATOR.keySet()) {
                        int indexOfSb = criteria.indexOf(sb);
                        if (indexOfSb != -1) {
                            symbol = sb;
                            indexOfSymbol = indexOfSb;
                            break;
                        }
                    }
                    if (symbol == null) {
                        throw new IllegalArgumentException("No comparison symbol found in"
                                + " criteria: " + criteria);
                    }
                    // key: "used"
                    final String key = criteria.substring(1, indexOfSymbol);
                    // value: "2"
                    final String value = criteria.substring(indexOfSymbol + symbol.length());
                    // ComparisonOperator for ">="
                    final ComparisonOperator co = COMPARISON_SYMBOL_TO_COMPARISON_OPERATOR.get(symbol);

                    objectFilter.addFilters(PropertyFilter.newBuilder()
                            .setPropertyName(key)
                            .setNumericFilter(NumericFilter.newBuilder()
                                    .setComparisonOperator(co)
                                    .setValue(Integer.valueOf(value))
                                    .build())
                            .build());
                } else if (criteria.contains("=")) {
                    // if no # provided, it means string by default, for example: "type=VMem"
                    String[] keyValue = criteria.split("=");
                    objectFilter.addFilters(
                        SearchProtoUtil.stringPropertyFilterExact(
                            keyValue[0], Collections.singletonList(keyValue[1]), true, false));
                } else {
                    // if no "=", it means this is final field, whose comparison operator and value
                    // are provided by UI in FilterApiDTO, for example: capacity
                    objectFilter.addFilters(createPropertyFilterForNormalToken(criteria,
                            inputType, filter));
                }
            }
            listFilter.setObjectFilter(objectFilter.build());
        }

        return PropertyFilter.newBuilder()
                .setPropertyName(listFieldName)
                .setListFilter(listFilter.build())
                .build();
    }

    public static boolean isRegexOperator(@Nonnull String operator) {
        return operator.equals(REGEX_MATCH) || operator.equals(REGEX_NO_MATCH);
    }

    public static boolean isPositiveMatchingOperator(@Nonnull String operator) {
        return operator.equals(REGEX_MATCH) || operator.equals(EQUAL);
    }

    /**
     * Create SearchFilter for a normal token (string/numeric) which may contain nested fields.
     * For example: currentToken: "virtualMachineInfoRepoDTO.numCpus"
     *
     * @param currentToken the token which may contain nested fields
     * @param inputType the type of the input from UI, which can be "*" (string) or "#" (number)
     * @param filter the FilterApiDTO which contains values provided by user in UI
     * @return PropertyFilter
     */
    private PropertyFilter createPropertyFilterForNormalToken(@Nonnull String currentToken,
            @Nonnull String inputType, @Nonnull FilterApiDTO filter) {
        final String[] nestedFields = currentToken.split(NESTED_FIELD_DELIMITER);
        // start from last field, create the innermost PropertyFilter
        String lastField = nestedFields[nestedFields.length - 1];
        PropertyFilter currentFieldPropertyFilter;
        switch (inputType) {
            case "s":
            case "s|*":
            case "*|s":
            case "*":
                // string matching
                // the input types "s" and "*" represent exact string matching
                // and regex matching respectively.  their combination allows for both.
                // all cases are treated in the same way here (the distinction is only
                // helpful to the UI side).  we can distinguish the cases by looking at
                // the operator
                final boolean positiveMatch = isPositiveMatchingOperator(filter.getExpType());
                if (isRegexOperator(filter.getExpType())) {
                    currentFieldPropertyFilter =
                        SearchProtoUtil.stringPropertyFilterRegex(
                            lastField,
                            filter.getExpVal(),
                            positiveMatch,
                            filter.getCaseSensitive());
                } else {
                    currentFieldPropertyFilter =
                        SearchProtoUtil.stringPropertyFilterExact(
                            lastField,
                            Arrays.stream(filter.getExpVal().split("\\|"))
                                    .collect(Collectors.toList()),
                            positiveMatch,
                            filter.getCaseSensitive());
                }
                break;
            case "#":
                // numeric comparison
                currentFieldPropertyFilter = SearchProtoUtil.numericPropertyFilter(lastField,
                        Long.valueOf(filter.getExpVal()),
                        COMPARISON_STRING_TO_COMPARISON_OPERATOR.get(filter.getExpType()));
                break;
            default:
                throw new UnsupportedOperationException("Input type: " + inputType +
                        " is not supported");
        }

        // process nested fields from second last in descending order
        for (int i = nestedFields.length - 2; i >= 0; i--) {
            currentFieldPropertyFilter = PropertyFilter.newBuilder()
                    .setPropertyName(nestedFields[i])
                    .setObjectFilter(ObjectFilter.newBuilder()
                            .addFilters(currentFieldPropertyFilter)
                            .build())
                    .build();
        }
        return currentFieldPropertyFilter;
    }


    /**
     * Context with parameters which SearchFilterProducer needs for all cases.
     */
    private static class SearchFilterContext {

        private final FilterApiDTO filter;

        private final Iterator<String> iterator;

        private final String entityType;

        private final String currentToken;

        // the first token of the elements defined in groupBuilderUsecases.json, for example:
        // "PhysicalMachine:displayName:PRODUCES:1", the first token is "PhysicalMachine"
        private final String firstToken;

        SearchFilterContext(@Nonnull FilterApiDTO filter, @Nonnull Iterator<String> iterator,
                        @Nonnull String entityType, @Nonnull String currentToken, @Nonnull String firstToken) {
            this.filter = Objects.requireNonNull(filter);
            this.iterator = Objects.requireNonNull(iterator);
            this.entityType = Objects.requireNonNull(entityType);
            this.currentToken = Objects.requireNonNull(currentToken);
            this.firstToken = Objects.requireNonNull(firstToken);
        }

        @Nonnull
        public FilterApiDTO getFilter() {
            return filter;
        }

        @Nonnull
        public Iterator<String> getIterator() {
            return iterator;
        }

        @Nonnull
        public String getEntityType() {
            return entityType;
        }

        @Nonnull
        public String getCurrentToken() {
            return currentToken;
        }

        public boolean isHopCountBasedTraverse(@Nonnull StoppingConditionOrBuilder stopper) {
            return !iterator.hasNext() && stopper.hasNumberHops();
        }

        /**
         * Check if this SearchFilter should filter by number of connected vertices. For example:
         * filter PMs by number of hosted VMs.
         * @return if should filter by number of connected vertices.
         */
        public boolean shouldFilterByNumConnectedVertices() {
            return TRAVERSAL_TYPES.contains(firstToken);
        }
    }

    /**
     * Processor for filter which has PRODUCES type of token.
     */
    @Immutable
    private static class TraversalFilterProcessor implements Function<SearchFilterContext, List<SearchFilter>> {

        @Override
        public List<SearchFilter> apply(SearchFilterContext context) {
            // add a traversal filter
            TraversalDirection direction = TraversalDirection.valueOf(context.getCurrentToken());
            final StoppingCondition.Builder stopperBuilder;
            final Iterator<String> iterator = context.getIterator();
            final String entityType = context.getEntityType();
            if (iterator.hasNext()) {
                final String currentToken = iterator.next();
                // An explicit stopper can either be the number of hops, or an
                // entity type. And note that hops number can not contains '+' or '-'.
                if (StringUtils.isNumeric(currentToken)) {
                    // For example: Produces:1:VirtualMachine
                    final int hops = Integer.valueOf(currentToken);
                    if (hops <= 0) {
                        throw new IllegalArgumentException("Illegal hops number " + hops
                                        + "; should be positive.");
                    }
                    stopperBuilder = StoppingCondition.newBuilder().setNumberHops(hops);
                    // set condition for number of connected vertices if required
                    if (context.shouldFilterByNumConnectedVertices()) {
                        setVerticesCondition(stopperBuilder, iterator.next(), context);
                    }
                } else {
                    // For example: Produces:VirtualMachine
                    stopperBuilder = buildStoppingCondition(currentToken);
                    // set condition for number of connected vertices if required
                    if (context.shouldFilterByNumConnectedVertices()) {
                        setVerticesCondition(stopperBuilder, currentToken, context);
                    }
                }
            } else {
                stopperBuilder = buildStoppingCondition(entityType);
            }

            TraversalFilter traversal = TraversalFilter.newBuilder()
                            .setTraversalDirection(direction)
                            .setStoppingCondition(stopperBuilder)
                            .build();
            final ImmutableList.Builder<SearchFilter> searchFilters = ImmutableList.builder();
            searchFilters.add(SearchProtoUtil.searchFilterTraversal(traversal));
            // add a final entity type filter if the last filer is a hop-count based traverse
            // and it's not a filter based on number of connected vertices
            // for example: get all PMs which hosted more than 2 VMs, we've already get all PMs
            // if it's a filter by number of connected vertices, we don't need to filter on PM type again
            if (context.isHopCountBasedTraverse(stopperBuilder) && !context.shouldFilterByNumConnectedVertices()) {
                searchFilters.add(SearchProtoUtil.searchFilterProperty(SearchProtoUtil
                                .entityTypeFilter(entityType)));
            }
            return searchFilters.build();
        }

        /**
         * Add vertices condition to the given StoppingCondition. For example, group of PMs by
         * number of hosted VMs, the stopping condition contains number of hops, which is 1. This
         * function add one more condition: filter by number of vms hosted by this PM.
         *
         * @param stopperBuilder the StoppingCondition builder to add vertices condition to
         * @param stoppingEntityType the entity type to count number of connected vertices for
         * when the traversal stops. for example: PMs by number of hosted VMs, then the
         * stoppingEntityType is the integer value of VM entity type
         * @param context the SearchFilterContext with parameters provided by user for the group
         */
        private void setVerticesCondition(@Nonnull StoppingCondition.Builder stopperBuilder,
                @Nonnull String stoppingEntityType, @Nonnull SearchFilterContext context) {
            int vertexEntityType = UIEntityType.fromString(stoppingEntityType).typeNumber();
            stopperBuilder.setVerticesCondition(VerticesCondition.newBuilder()
                    .setNumConnectedVertices(NumericFilter.newBuilder()
                            .setValue(Long.valueOf(context.getFilter().getExpVal()))
                            .setComparisonOperator(COMPARISON_STRING_TO_COMPARISON_OPERATOR.get(
                                    context.getFilter().getExpType())))
                    .setEntityType(vertexEntityType)
                    .build());
        }
    }

    /**
     * Convert a {@link EntityFilter} to a list of FilterApiDTO.
     *
     * @param entityFilter {@link EntityFilter} A message contains all information
     *  to represent a static or dynamic group.
     * @return a list of FilterApiDTO which contains different filter rules for dynamic group
     */
    public List<FilterApiDTO> convertToFilterApis(EntityFilter entityFilter) {
        return entityFilter
                        .getSearchParametersCollection()
                        .getSearchParametersList().stream()
                        .map(this::toFilterApiDTO)
                        .collect(Collectors.toList());
    }

    /**
     * Converts SearchParameters object to FilterApiDTO.
     *
     * @param searchParameters represent one search query
     * @return The {@link FilterApiDTO} object which contains filter rule for dynamic group
     */
    private FilterApiDTO toFilterApiDTO(@Nonnull SearchParameters searchParameters) {

        final FilterApiDTO filterApiDTO = new FilterApiDTO();
        final SearchParameters.FilterSpecs sourceFilter = searchParameters.getSourceFilterSpecs();
        filterApiDTO.setExpType(Objects.requireNonNull(sourceFilter.getExpressionType()));
        filterApiDTO.setExpVal(Objects.requireNonNull(sourceFilter.getExpressionValue()));
        filterApiDTO.setFilterType(Objects.requireNonNull(sourceFilter.getFilterType()));
        filterApiDTO.setCaseSensitive(Objects.requireNonNull(sourceFilter.getIsCaseSensitive()));
        return filterApiDTO;
    }
}

package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
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
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.GroupFilter;
import com.vmturbo.common.protobuf.search.Search.GroupFilter.EntityToGroupType;
import com.vmturbo.common.protobuf.search.Search.LogicalOperator;
import com.vmturbo.common.protobuf.search.Search.MultiTraversalFilter;
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
import com.vmturbo.common.protobuf.search.UIBooleanFilter;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.utils.ThrowingFunction;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;


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
    /** Key of the criteria to query business accounts by Name. */
    public static final String ACCOUNT_NAME = "businessAccountByName";
    /** Key of the criteria to query business accounts by OID. */
    public static final String ACCOUNT_OID = "BusinessAccount:oid:OWNS:1";
    /** Key of the criteria to query resource groups by ids. */
    public static final String MEMBER_OF_RESOURCE_GROUP_OID = "MemberOf:ResourceGroup:uuid";
    /** Key of the criteria to query resource groups by names. */
    public static final String MEMBER_OF_RESOURCE_GROUP_NAME = "MemberOf:ResourceGroup:displayName";
    /** Key of the criteria to query resource groups by ids. */
    public static final String OWNER_OF_RESOURCE_GROUP_OID = "OwnerOf:ResourceGroup:uuid";
    /** Key of the criteria to query billing families by ids. */
    public static final String MEMBER_OF_BILLING_FAMILY_OID = "MemberOf:BillingFamily:uuid";
    /** key of the criteria to query WorkloadController entities by controller types. */
    public static final String WORKLOAD_CONTROLLER_TYPE = SearchableProperties.WC_INFO_REPO_DTO_PROPERTY_NAME
        + "." + SearchableProperties.CONTROLLER_TYPE;
    /** key of the criteria to query Container entities by controller types. */
    public static final String CONTAINER_WORKLOAD_CONTROLLER_TYPE = "WorkloadController:" + WORKLOAD_CONTROLLER_TYPE + ":PRODUCES:ContainerPod:PRODUCES";
    /** key of the criteria to query ContainerPod entities by controller types. */
    public static final String CONTAINER_POD_WORKLOAD_CONTROLLER_TYPE = "WorkloadController:" + WORKLOAD_CONTROLLER_TYPE + ":PRODUCES";
    /** key of the criteria to query ContainerSpec entities by controller types. */
    public static final String CONTAINER_SPEC_WORKLOAD_CONTROLLER_TYPE = "WorkloadController:" + WORKLOAD_CONTROLLER_TYPE + ":AGGREGATES";
    /** Key of the criteria to query Services by kubernetes service types. */
    public static final String KUBERNETES_SERVICE_TYPE = SearchableProperties.SERVICE_INFO_REPO_DTO_PROPERTY_NAME
            + "." + SearchableProperties.KUBERNETES_SERVICE_TYPE;
    public static final String STATE = "state";
    public static final String USER_DEFINED_ENTITY = "UserDefinedEntity";
    public static final String NETWORKS = "networks";
    public static final String CONNECTED_NETWORKS_FIELD = "connectedNetworks";
    public static final String CONNECTED_STORAGE_TIER_FILTER_PATH = "StorageTier:oid:PRODUCES:1";
    public static final String VOLUME_ATTACHMENT_STATE_FILTER_PATH = SearchableProperties.VOLUME_REPO_DTO +
        "." + SearchableProperties.VOLUME_ATTACHMENT_STATE;
    public static final String REGION_FILTER_PATH = "Region:oid:AGGREGATES:VirtualVolume";
    private static final String UUID_TOKEN = "uuid";

    private static final String CONSUMES = "CONSUMES";
    private static final String PRODUCES = "PRODUCES";
    private static final String CONNECTED_TO = "CONNECTED_TO";
    private static final String CONNECTED_FROM = "CONNECTED_FROM";
    private static final String OWNS = "OWNS";
    private static final String OWNED_BY = "OWNED_BY";
    private static final String AGGREGATES = "AGGREGATES";
    private static final String AGGREGATED_BY = "AGGREGATED_BY";

    public static final String EQUAL = "EQ";
    public static final String NOT_EQUAL = "NEQ";
    public static final String GREATER_THAN = "GT";
    public static final String LESS_THAN = "LT";
    public static final String GREATER_THAN_OR_EQUAL = "GTE";
    public static final String LESS_THAN_OR_EQUAL = "LTE";
    public static final String MULTIPLE_OF = "MO";
    public static final String NOT_MULTIPLE_OF = "NMO";
    public static final String REGEX_MATCH = "RXEQ";
    public static final String REGEX_NO_MATCH = "RXNEQ";

    public static final String ELEMENTS_DELIMITER = ":";
    public static final String NESTED_FIELD_DELIMITER = "\\.";

    public static final String ACCOUNT_PROBE_TYPE_FILTER = "businessAccountProbeType";

    private static final String MEMBER_OF = "MemberOf";

    private static final String OWNER_OF = "OwnerOf";

    // set of supported traversal types, the string should be the same as groupBuilderUsecases.json
    private static final Set<String> TRAVERSAL_TYPES = ImmutableSet.of(
            CONSUMES, PRODUCES, CONNECTED_TO, CONNECTED_FROM, OWNS);

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
                    .put(MULTIPLE_OF, ComparisonOperator.MO)
                    .put(NOT_MULTIPLE_OF, ComparisonOperator.NMO)
                    .build();

    private static final Map<String, ThrowingFunction<SearchFilterContext, List<SearchFilter>, OperationFailedException>>
                FILTER_TYPES_TO_PROCESSORS = initFilterTypesToProcessor();
    private static final MultiRelationsFilterProcessor MULTI_RELATIONS_FILTER_PROCESSOR =
                    new MultiRelationsFilterProcessor();

    private final ThinTargetCache thinTargetCache;

    private final GroupUseCaseParser groupUseCaseParser;

    /**
     * Constructor for {@link EntityFilterMapper}.
     * @param groupUseCaseParser group use case parser.
     * @param thinTargetCache for retrieving targets without making a gRPC call.
     */
    public EntityFilterMapper(GroupUseCaseParser groupUseCaseParser,  ThinTargetCache thinTargetCache) {
        this.groupUseCaseParser = groupUseCaseParser;
        this.thinTargetCache = thinTargetCache;
    }

    private static Map<String, ThrowingFunction<SearchFilterContext, List<SearchFilter>, OperationFailedException>>
                    initFilterTypesToProcessor() {
        final ThrowingFunction<SearchFilterContext, List<SearchFilter>, OperationFailedException>
                        traversalFilterProcessor = new TraversalFilterProcessor();
        final ImmutableMap.Builder<String, ThrowingFunction<SearchFilterContext, List<SearchFilter>, OperationFailedException>>
                        filterTypesToProcessors = new ImmutableMap.Builder<>();
        filterTypesToProcessors.put(StringConstants.TAGS_ATTR, EntityFilterMapper::getTagProcessor);
        filterTypesToProcessors.put(StringConstants.CLUSTER, EntityFilterMapper::getGroupFilterProcessor);
        filterTypesToProcessors.put(MEMBER_OF, EntityFilterMapper::getGroupFilterProcessor);
        filterTypesToProcessors.put(OWNER_OF, EntityFilterMapper::getGroupFilterProcessor);
        filterTypesToProcessors.put(NETWORKS, EntityFilterMapper::getNetworkProcessor);
        filterTypesToProcessors.put(SearchableProperties.VENDOR_ID, EntityFilterMapper::getVendorIdProcessor);
        filterTypesToProcessors.put(CONSUMES, traversalFilterProcessor);
        filterTypesToProcessors.put(PRODUCES, traversalFilterProcessor);
        filterTypesToProcessors.put(CONNECTED_FROM, traversalFilterProcessor);
        filterTypesToProcessors.put(CONNECTED_TO, traversalFilterProcessor);
        filterTypesToProcessors.put(OWNS, traversalFilterProcessor);
        filterTypesToProcessors.put(OWNED_BY, traversalFilterProcessor);
        filterTypesToProcessors.put(AGGREGATES, traversalFilterProcessor);
        filterTypesToProcessors.put(AGGREGATED_BY, traversalFilterProcessor);
        return filterTypesToProcessors.build();
    }


    private static Map<String, ComparisonOperator> initComparisonSymbolToOperatorMap() {
        Map<String, ComparisonOperator> symbolToOperator = new LinkedHashMap<>();
        symbolToOperator.put("!=", ComparisonOperator.NE);
        symbolToOperator.put(">=", ComparisonOperator.GTE);
        symbolToOperator.put("<=", ComparisonOperator.LTE);
        symbolToOperator.put("=", ComparisonOperator.EQ);
        symbolToOperator.put(">", ComparisonOperator.GT);
        symbolToOperator.put("<", ComparisonOperator.LT);
        symbolToOperator.put("%", ComparisonOperator.MO);
        symbolToOperator.put("!%", ComparisonOperator.NMO);
        return Collections.unmodifiableMap(symbolToOperator);
    }

    private static List<SearchFilter> getTagProcessor(SearchFilterContext context) {
        //TODO: the expression value coming from the UI is currently unsanitized.
        // It is assumed that the tag keys and values do not contain characters such as = and |.
        // This is reported as a JIRA issue OM-39039.
        return Collections.singletonList(SearchProtoUtil.searchFilterProperty(getTagsFilter(context)));
    }

    @Nonnull
    private static PropertyFilter getTagsFilter(@Nonnull final SearchFilterContext context) {
        final String operator = context.getFilter().getExpType();
        final boolean positiveMatch = isPositiveMatchingOperator(operator);
        if (!context.isExactMatching()) {
            // regex match is required
            return mapPropertyFilterForMultimapsRegex(StringConstants.TAGS_ATTR,
                            context.getFilter().getExpVal(),
                            positiveMatch);
        } else {
            // exact match is required
            return mapPropertyFilterForMultimapsExact(
                            StringConstants.TAGS_ATTR,
                            context.getFilter().getExpVal(),
                            positiveMatch);
        }
    }

    @Nonnull
    private static List<SearchFilter> getGroupFilterProcessor(@Nonnull SearchFilterContext context) {
        final String currentToken = context.getCurrentToken();
        final EntityToGroupType entityToGroupType;
        switch (currentToken) {
            case OWNER_OF:
                entityToGroupType = EntityToGroupType.OWNER_OF;
                break;
            default:
                entityToGroupType = EntityToGroupType.MEMBER_OF;
                break;
        }
        String groupTypeToken;
        if (currentToken.equals(MEMBER_OF) || currentToken.equals(OWNER_OF)) {
            groupTypeToken = context.getIterator().next();
        } else {
            groupTypeToken = context.getCurrentToken();
        }
        final GroupType groupType = GroupMapper.API_GROUP_TYPE_TO_GROUP_TYPE.get(groupTypeToken);
        if (groupType == null) {
            throw new IllegalArgumentException("Unknown group type " + groupTypeToken);
        }
        final FilterApiDTO filter = context.getFilter();
        final boolean isPositiveMatch = isPositiveMatchingOperator(filter.getExpType());
        final String propertyName = translateToken(context.getIterator().next());
        PropertyFilter groupSpecifier;
        if (StringConstants.TAGS_ATTR.equals(propertyName)) {
            groupSpecifier = getTagsFilter(context);
        } else {
            groupSpecifier = context.isExactMatching() ?
                    SearchProtoUtil.stringPropertyFilterExact(propertyName,
                            Arrays.asList(filter.getExpVal()), isPositiveMatch,
                            filter.getCaseSensitive()) :
                    SearchProtoUtil.stringPropertyFilterRegex(propertyName, filter.getExpVal(),
                            isPositiveMatch, filter.getCaseSensitive());
        }

        final GroupFilter groupFilter = GroupFilter.newBuilder()
                .setGroupSpecifier(groupSpecifier)
                .setEntityToGroupType(entityToGroupType)
                .setGroupType(groupType)
                .build();
        return Collections.singletonList(createSearchFilter(groupFilter));
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

    private static List<SearchFilter> getVendorIdProcessor(SearchFilterContext context) {
        final StringFilter stringFilter =
            SearchProtoUtil.stringFilterRegex(
                context.getFilter().getExpVal(),
                context.getFilter().getExpType().equals(REGEX_MATCH),
                context.getFilter().getCaseSensitive());
        return Collections.singletonList(SearchProtoUtil.searchFilterProperty(
            PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.VENDOR_ID)
                .setListFilter(
                    ListFilter.newBuilder()
                        .setStringFilter(stringFilter))
                .build()));
    }

    private final SetOnce<String> udtOid = new SetOnce();

    /**
     * Processor for "User Defined Entity" filter.
     *
     * @param context necessary parameters for filter.
     * @return list of {@SearchFilter} to use for querying the repository.
     */
    private List<SearchFilter> getUserDefinedEntityProcessor(SearchFilterContext context) {
        final FilterApiDTO filter = context.getFilter();
        final String criteriaOption = filter.getExpVal();
        final boolean positiveMatch = !(criteriaOption.equals(UIBooleanFilter.TRUE.apiStr())
                ^ isPositiveMatchingOperator(filter.getExpType()));
        if (!udtOid.getValue().isPresent()) {
            udtOid.trySetValue(thinTargetCache.getAllTargets()
                    .stream()
                    .filter(targetInfo ->
                        targetInfo.probeInfo().type().equals(SDKProbeType.UDT.getProbeType()))
                    .map(ThinTargetInfo::oid)
                    .map(String::valueOf)
                    .findFirst().orElse(null));
        }

        return Collections.singletonList(SearchProtoUtil.searchFilterProperty(
                PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.EXCLUSIVE_DISCOVERING_TARGET)
                        .setStringFilter(
                                SearchProtoUtil.stringFilterExact(
                                    udtOid.getValue().map(Collections::singletonList)
                                            .orElse(Collections.emptyList()),
                                    positiveMatch,
                                    context.getFilter().getCaseSensitive()))
                        .build()));
    }

    /**
     * Convert a list of  {@link FilterApiDTO} to a list of search parameters. Right now the
     * entity type sources have inconsistency between search service with group service requests.
     * For search service request, UI use class name field store entityType, but for creating
     * group request, UI use groupType field store entityType.
     *
     * @param criteriaList list of {@link FilterApiDTO}  received from the UI
     * @param entityTypes the name of entity types, such as VirtualMachine
     * @return list of search parameters to use for querying the repository
     */
    public Collection<SearchParameters> convertToSearchParameters(@Nonnull List<FilterApiDTO> criteriaList,
                                                            @Nonnull List<String> entityTypes)
                    throws OperationFailedException {
        return convertToSearchParameters(criteriaList, entityTypes, null);
    }

    /**
     * Convert a list of  {@link FilterApiDTO} to a list of search parameters. Right now the
     * entity type sources have inconsistency between search service with group service requests.
     * For search service request, UI use class name field store entityType, but for creating
     * group request, UI use groupType field store entityType.
     *
     * @param filters list of {@link FilterApiDTO}  received from the UI
     * @param entityTypes the name of entity types, such as VirtualMachine
     * @param nameQuery user specified search query for entity name. If it is not null, it will be
     *                  converted to a entity name filter. It is either a regex or a quoted exact match.
     *                  Also it can be a regex such as to correspond to CONTAINS pattern matching.
     * @return list of search parameters to use for querying the repository
     */
    public Collection<SearchParameters> convertToSearchParameters(@Nonnull List<FilterApiDTO> filters,
                                                            @Nonnull List<String> entityTypes,
                                                            @Nullable String nameQuery)
                    throws OperationFailedException {
        if (filters == null || filters.isEmpty()) {
            return Collections.singletonList(searchParametersForEmptyCriteria(entityTypes,
                            nameQuery));
        }
        final Collection<SearchParameters> result = new ArrayList<>(filters.size());
        for (FilterApiDTO filter : filters) {
            result.add(filter2parameters(filter, entityTypes,
                            nameQuery));
        }
        return result;
    }

    /**
     * Convert a list of  {@link FilterApiDTO} to a list of search parameters. Right now the
     * entity type sources have inconsistency between search service with group service requests.
     * For search service request, UI use class name field store entityType, but for creating
     * group request, UI use groupType field store entityType.
     *
     * @param criteriaList list of {@link FilterApiDTO}  received from the UI
     * @param entityType the name of entity type, such as VirtualMachine
     * @return list of search parameters to use for querying the repository
     */
    public Collection<SearchParameters> convertToSearchParameters(@Nonnull List<FilterApiDTO> criteriaList,
                                                            @Nonnull String entityType)
                    throws OperationFailedException {
        return convertToSearchParameters(criteriaList, Collections.singletonList(entityType));
    }

    /**
     * Convert a list of  {@link FilterApiDTO} to a list of search parameters. Right now the
     * entity type sources have inconsistency between search service with group service requests.
     * For search service request, UI use class name field store entityType, but for creating
     * group request, UI use groupType field store entityType.
     *
     * @param criteriaList list of {@link FilterApiDTO}  received from the UI
     * @param entityType the name of entity type, such as VirtualMachine
     * @return list of search parameters to use for querying the repository
     */
    public Collection<SearchParameters> convertToSearchParameters(@Nonnull List<FilterApiDTO> criteriaList,
                                                            @Nonnull String entityType,
                                                            @Nullable String nameQuery)
                    throws OperationFailedException {
        return convertToSearchParameters(criteriaList, Collections.singletonList(entityType), nameQuery);
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
     * <p>If parsing fails, then the filter that is generated
     * should reject all entities.
     * </p>
     *
     * @param propName property name to use for the search.
     * @param expField expression field coming from the UI.
     * @param positiveMatch if false, then negate the result of the filter.
     * @return the property filter
     */
    @Nonnull
    public static PropertyFilter mapPropertyFilterForMultimapsExact(@Nonnull String propName,
                                                                    @Nonnull String expField,
                                                                    boolean positiveMatch) {
        final List<String> keyValuePairs = splitWithEscapes(expField, '|');
        String key = null;
        final List<String> values = new ArrayList<>();
        for (String kvp : keyValuePairs) {
            final List<String> kv = splitWithEscapes(kvp, '=');
            if (kv.size() != 2) {
                logger.error("Cannot parse {} as a key/value pair", kvp);
                return REJECT_ALL_PROPERTY_FILTER;
            }
            if (key == null) {
                key = removeEscapes(kv.get(0));
            } else if (!key.equals(removeEscapes(kv.get(0)))) {
                logger.error("Map filter {} contains more than one key", expField);
                return REJECT_ALL_PROPERTY_FILTER;
            }
            if (!kv.get(1).isEmpty()) {
                values.add(removeEscapes(kv.get(1)));
            } else {
                logger.error("Cannot parse {} as a key/value pair", kvp);
                return REJECT_ALL_PROPERTY_FILTER;
            }
        }

        if (key == null) {
            logger.error("Cannot parse {} to generate a string filter", expField);
            return REJECT_ALL_PROPERTY_FILTER;
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
     * A filter that rejects every entity (assuming that no entity has oid=0)
     */
    public static final PropertyFilter REJECT_ALL_PROPERTY_FILTER =
            PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.OID)
                .setNumericFilter(NumericFilter.newBuilder()
                                    .setValue(0L)
                                    .setComparisonOperator(ComparisonOperator.EQ))
                .build();

    /**
     * Create a map filter for the specified property name
     * and specified regex coming from the UI.
     *
     * <p>This filter should match the expression key=value
     * to the regex expression.</p>
     *
     * <p>The filter created allows for multimap properties.  The values of such
     * properties are maps, in which multiple values may correspond to a single key.
     * For example key "user" -> ["peter" and "paul"].</p>
     *
     * @param propName the property name to use for the search.
     * @param regex the regex to match keys and values against.
     * @param positiveMatch if false, then negate the result of the filter.
     * @return the property filter.
     */
    @Nonnull
    public static PropertyFilter mapPropertyFilterForMultimapsRegex(
            @Nonnull String propName, @Nonnull String regex, boolean positiveMatch) {
        final PropertyFilter propertyFilter = PropertyFilter.newBuilder()
            .setPropertyName(propName)
            .setMapFilter(MapFilter.newBuilder()
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
        final String breakingCharacterPattern = SearchProtoUtil
                        .escapeSpecialCharactersInLiteral(Character.toString(breakingChar));

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
    private SearchParameters searchParametersForEmptyCriteria(@Nonnull final List<String>
            entityType, @Nullable final String nameQuery) {
        PropertyFilter byType = SearchProtoUtil.entityTypeFilter(entityType);
        final SearchParameters.Builder searchParameters = SearchParameters.newBuilder()
                        .setStartingFilter(byType);
        if (!StringUtils.isEmpty(nameQuery)) {
            searchParameters.addSearchFilter(SearchProtoUtil.searchFilterProperty(
                            SearchProtoUtil.nameFilterRegex(nameQuery )));
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
     * @param entityTypes entity types if the filter
     * @param nameQuery user specified search query for entity name. If it is not null, it will be
     *                  converted to a entity name filter.
     * @return parameters full search parameters
     */
    private SearchParameters filter2parameters(@Nonnull FilterApiDTO filter,
                                               @Nonnull List<String> entityTypes,
                                               @Nullable String nameQuery)
                    throws OperationFailedException {
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
        if (ApiEntityType.fromString(firstToken) != ApiEntityType.UNKNOWN) {
            parametersBuilder.setStartingFilter(SearchProtoUtil.entityTypeFilter(firstToken));
        } else {
            parametersBuilder.setStartingFilter(SearchProtoUtil.entityTypeFilter(entityTypes));
            iterator = elements.iterator();
        }

        final ImmutableList.Builder<SearchFilter> searchFilters = new ImmutableList.Builder<>();
        while (iterator.hasNext()) {
            searchFilters.addAll(processToken(filter, entityTypes, iterator, useCase.getInputType(),
                            firstToken));
        }
        if (!StringUtils.isEmpty(nameQuery)) {
            // nameQuery is a string regex that can either implement
            // a REGEX or a CONTAINS or EXACT pattern mathing.
            searchFilters.add(SearchProtoUtil.searchFilterProperty(
                SearchProtoUtil.nameFilterRegex(nameQuery)));
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
    private static boolean isListToken(@Nonnull String token) {
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
                                            @Nonnull List<String> entityType,
                                            @Nonnull Iterator<String> iterator,
                                            @Nonnull String inputType,
                                            @Nonnull String firstToken)
                    throws OperationFailedException {
        final String currentToken = iterator.next();

        final String operator = filter.getExpType();
        final SearchFilterContext filterContext =
                new SearchFilterContext(filter, iterator, entityType, currentToken, firstToken,
                        !isRegexOperator(operator), false);
        final ThrowingFunction<SearchFilterContext, List<SearchFilter>, OperationFailedException>
                        filterApiDtoProcessor = FILTER_TYPES_TO_PROCESSORS.get(currentToken);

        if (filterApiDtoProcessor != null) {
            return filterApiDtoProcessor.apply(filterContext);
        } else if (currentToken.equals(USER_DEFINED_ENTITY)) {
            return getUserDefinedEntityProcessor(filterContext);
        } else if (ApiEntityType.fromString(currentToken) != ApiEntityType.UNKNOWN) {
            return ImmutableList.of(SearchProtoUtil
                            .searchFilterProperty(SearchProtoUtil.entityTypeFilter(currentToken)));
        } else if (MULTI_RELATIONS_FILTER_PROCESSOR.test(filterContext)) {
            return MULTI_RELATIONS_FILTER_PROCESSOR.apply(filterContext);
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
                        Long.parseLong(filter.getExpVal()),
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
                                    .setValue(Integer.parseInt(value))
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

    private static String translateToken(@Nonnull final String token) {
        return token.equals(UUID_TOKEN) ? SearchableProperties.OID : token;
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
                        Long.parseLong(filter.getExpVal()),
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

        private final List<String> entityTypes;

        private final String currentToken;

        // the first token of the elements defined in groupBuilderUsecases.json, for example:
        // "PhysicalMachine:displayName:PRODUCES:1", the first token is "PhysicalMachine"
        private final String firstToken;

        private final boolean exactMatching;

        private final boolean limitTargetEntityType;

        SearchFilterContext(@Nonnull FilterApiDTO filter, @Nonnull Iterator<String> iterator,
                        @Nonnull List<String> entityTypes, @Nonnull String currentToken,
                        @Nonnull String firstToken, boolean exactMatching,
                        boolean limitTargetEntityType) {
            this.filter = Objects.requireNonNull(filter);
            this.iterator = Objects.requireNonNull(iterator);
            this.entityTypes = Objects.requireNonNull(entityTypes);
            this.currentToken = Objects.requireNonNull(currentToken);
            this.firstToken = Objects.requireNonNull(firstToken);
            this.exactMatching = Objects.requireNonNull(exactMatching);
            this.limitTargetEntityType = limitTargetEntityType;
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
        public List<String> getEntityTypes() {
            return entityTypes;
        }

        @Nonnull
        public String getCurrentToken() {
            return currentToken;
        }

        public boolean isExactMatching() {
            return exactMatching;
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
     * {@link RelationsFilterProcessor} parent for filters which have to go through topology graph
     * and find the related entities.
     */
    private abstract static class RelationsFilterProcessor implements
                    ThrowingFunction<SearchFilterContext, List<SearchFilter>, OperationFailedException> {

        @Override
        public List<SearchFilter> apply(SearchFilterContext context)
                        throws OperationFailedException {
            // add a traversal filter
            final Collection<TraversalDirection> directions = getDirections(context);
            final StoppingCondition.Builder stopperBuilder = createStoppingCondition(context);
            final SearchFilter traversalFilters =
                            createTraversalFilter(directions, stopperBuilder);
            final ImmutableList.Builder<SearchFilter> searchFilters = ImmutableList.builder();
            searchFilters.add(traversalFilters);
            // add a final entity type filter if the last filter is a hop-count based traverse
            // and it's not a filter based on number of connected vertices
            // for example: get all PMs which hosted more than 2 VMs, we've already get all PMs
            // if it's a filter by number of connected vertices, we don't need to filter on PM type again
            if (context.isHopCountBasedTraverse(stopperBuilder) && !context.shouldFilterByNumConnectedVertices()) {
                searchFilters.add(SearchProtoUtil.searchFilterProperty(SearchProtoUtil
                                .entityTypeFilter(context.getEntityTypes())));
            }
            return searchFilters.build();
        }

        /**
         * Creates {@link SearchFilter} which contains traversal limitations.
         *
         * @param directions collection of traversal directions that we should pass
         *                 while walking topology graph.
         * @param stopperBuilder builder which contains stopping condition for
         *                 walking over topology graph.
         * @return instance of {@link SearchFilter} which contains traversal limitations.
         */
        @Nonnull
        protected abstract SearchFilter createTraversalFilter(
                        @Nonnull Collection<TraversalDirection> directions,
                        @Nonnull StoppingCondition.Builder stopperBuilder);

        /**
         * Transform tokens from the filter definition into {@link Collection} of {@link
         * TraversalDirection} that we have visit while we are walking over topology graph.
         *
         * @param context contains information about search filter instance which is
         *                 currently processing.
         * @return collection of {@link TraversalDirection}s that we will visit during
         *                 walking over topology graph.
         */
        @Nonnull
        protected abstract Collection<TraversalDirection> getDirections(
                        @Nonnull SearchFilterContext context);

        private static StoppingCondition.Builder createStoppingCondition(SearchFilterContext context)
                        throws OperationFailedException {
            final StoppingCondition.Builder result;
            final Iterator<String> iterator = context.getIterator();
            final List<String> entityTypes = context.getEntityTypes();
            if (iterator.hasNext()) {
                final String currentToken = iterator.next();
                // An explicit stopper can either be the number of hops, or an
                // entity type. And note that hops number can not contains '+' or '-'.
                if (StringUtils.isNumeric(currentToken)) {
                    // For example: Produces:1:VirtualMachine
                    final int hops = Integer.parseInt(currentToken);
                    if (hops <= 0) {
                        throw new OperationFailedException(
                                        String.format("Illegal hops number %s; should be positive.",
                                                        hops));
                    }
                    result = StoppingCondition.newBuilder().setNumberHops(hops);
                    // set condition for number of connected vertices if required
                    if (context.shouldFilterByNumConnectedVertices()) {
                        setVerticesCondition(result, iterator.next(), context);
                    }
                } else {
                    // For example: Produces:VirtualMachine
                    result = buildStoppingCondition(Collections.singletonList(currentToken));
                    // set condition for number of connected vertices if required
                    if (context.shouldFilterByNumConnectedVertices()) {
                        setVerticesCondition(result, currentToken, context);
                    }
                }
            } else {
                result = buildStoppingCondition(entityTypes);
            }
            return result;
        }

        private static StoppingCondition.Builder buildStoppingCondition(
                        Collection<String> currentToken) {
            return StoppingCondition.newBuilder().setStoppingPropertyFilter(
                            SearchProtoUtil.entityTypeFilter(currentToken));
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
        private static void setVerticesCondition(@Nonnull StoppingCondition.Builder stopperBuilder,
                        @Nonnull String stoppingEntityType, @Nonnull SearchFilterContext context)
                        throws OperationFailedException {
            final int vertexEntityType = ApiEntityType.fromString(stoppingEntityType).typeNumber();
            final long numericFilterValue = getNumericFilterValue(context);
            final NumericFilter.Builder numericFilter = NumericFilter.newBuilder()
                            .setValue(numericFilterValue)
                            .setComparisonOperator(COMPARISON_STRING_TO_COMPARISON_OPERATOR
                                            .get(context.getFilter().getExpType()));
            stopperBuilder.setVerticesCondition(
                            VerticesCondition.newBuilder().setNumConnectedVertices(numericFilter)
                                            .setEntityType(vertexEntityType).build());
        }

        private static long getNumericFilterValue(@Nonnull SearchFilterContext context)
                        throws OperationFailedException {
            final FilterApiDTO filter = context.getFilter();
            final String rawValue = filter.getExpVal();
            try {
                return Long.parseLong(rawValue);
            } catch (NumberFormatException ex) {
                throw new OperationFailedException(
                                String.format("Cannot parse numeric value from '%s' to create vertices condition for '%s' filter",
                                                rawValue, filter));
            }
        }
    }

    /**
     * {@link MultiRelationsFilterProcessor} handles more than one relationship in the filter while
     * walking over the topology graph and combines results of the walking, by default results will
     * be combined with union operation. Please, note that this filter processor will work correctly
     * only in case it is processing relations with the same directions, i.e. CONNECTED_FROM and
     * CONSUMES, both are pointing to elements that are laying higher in the supply chain than the
     * source entity type. Please, consider filter for VV as an example:
     * Storage:displayName:CONNECTED_FROM|CONSUMES:1
     * source type will be 'Storage'
     * CONNECTED_FROM and CONSUMES relationships both pointing to the entities which are higher in
     * the supply chain definition:
     *  VV   VM
     *  |    |
     *  \   /
     *    S
     * In this case we will take all VV which are fitting in those relationships.
     *
     * In case relationships with opposite directions will be specified, then only one of them will
     * work, which is providing entities with the same type that filter is targeted on:
     * Storage:displayName:CONNECTED_FROM|PRODUCES:1
     *        VV   VM
     *        |    |
     *        \   /
     *          S
     *          |
     *         SC
     * In this case we will take ony VVs and we will not even consider StorageControllers from the
     * bottom of the supply chain.
     */
    private static class MultiRelationsFilterProcessor extends RelationsFilterProcessor
                    implements Predicate<SearchFilterContext> {
        private static final Pattern OPERATION_SPLITERATOR = Pattern.compile("\\||&");

        @Nonnull
        @Override
        protected SearchFilter createTraversalFilter(
                        @Nonnull Collection<TraversalDirection> directions,
                        @Nonnull StoppingCondition.Builder stopperBuilder) {
            final SearchFilter.Builder result = SearchFilter.newBuilder();
            final MultiTraversalFilter.Builder multiTraversalFilterBuilder =
                            MultiTraversalFilter.newBuilder().setOperator(LogicalOperator.OR);
            directions.stream().map(td -> TraversalFilter.newBuilder().setTraversalDirection(td)
                            .setStoppingCondition(stopperBuilder))
                            .forEach(multiTraversalFilterBuilder::addTraversalFilter);
            result.setMultiTraversalFilter(multiTraversalFilterBuilder);
            return result.build();
        }

        @Nonnull
        @Override
        protected Collection<TraversalDirection> getDirections(
                        @Nonnull SearchFilterContext context) {
            return getOperands(context).stream().map(TraversalDirection::valueOf)
                            .collect(Collectors.toSet());
        }

        private static Collection<String> getOperands(@Nonnull SearchFilterContext context) {
            return Arrays.stream(OPERATION_SPLITERATOR.split(context.getCurrentToken()))
                            .collect(Collectors.toSet());
        }

        @Override
        public boolean test(SearchFilterContext context) {
            return FILTER_TYPES_TO_PROCESSORS.keySet().containsAll(getOperands(context));
        }
    }

    /**
     * Processor for filter which has PRODUCES type of token.
     */
    @Immutable
    private static class TraversalFilterProcessor extends RelationsFilterProcessor {
        @Nonnull
        @Override
        protected SearchFilter createTraversalFilter(
                        @Nonnull Collection<TraversalDirection> directions,
                        @Nonnull StoppingCondition.Builder stopperBuilder) {
            final TraversalDirection direction = directions.iterator().next();
            final TraversalFilter traversal =
                            TraversalFilter.newBuilder().setTraversalDirection(direction)
                                            .setStoppingCondition(stopperBuilder).build();
            return SearchProtoUtil.searchFilterTraversal(traversal);
        }

        @Nonnull
        @Override
        protected Collection<TraversalDirection> getDirections(@Nonnull SearchFilterContext context) {
            return Collections.singleton(TraversalDirection.valueOf(context.getCurrentToken()));
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

    /**
     * Wrap an instance of {@link GroupFilter} with a {@link SearchFilter}.
     *
     * @param groupFilter the group filter to wrap
     * @return a search filter that wraps the argument
     */
    @Nonnull
    private static SearchFilter createSearchFilter(@Nonnull GroupFilter groupFilter) {
        return SearchFilter.newBuilder().setGroupFilter(groupFilter).build();
    }

    /**
     * @param key: the key that we want to seee if exists in map
     * @return true if it's a regex operator or COMPARISON_STRING_TO_COMPARISON_OPERATOR contains the key, else false
     */
    public static boolean checkIfValidComparisonOperator(String key) {
        return key.equals(REGEX_MATCH)
                || key.equals(REGEX_NO_MATCH)
                || (COMPARISON_STRING_TO_COMPARISON_OPERATOR.get(key) != null);
    }

    /**
     * @param logicalOperator: the key that we want to seee if exists in map
     * @return true if it's a valid logical operator
     */
    public static boolean checkIfValidLogicalOperator(String logicalOperator) {
        return logicalOperator.equalsIgnoreCase(LogicalOperator.AND.name())
                || logicalOperator.equalsIgnoreCase(LogicalOperator.OR.name())
                || logicalOperator.equalsIgnoreCase(LogicalOperator.XOR.name());
    }

    /**
     * @return a set of the keys of COMPARISON_STRING_TO_COMPARISON_OPERATOR map
     */
    public static Set<String> getComparisonOperators() {
        return COMPARISON_STRING_TO_COMPARISON_OPERATOR.keySet();
    }

    /**
     * @param inputDTO: the inpuDTO which we want to check. Specifically we check the
     * CriteriaList and to see if all filters in that list are valid. More checks could be
     * added in the future, if there is a need for more sanity checks.
     */
    public static void checkInputDTOParameters(GroupApiDTO inputDTO) throws IllegalArgumentException{
        inputDTO.getCriteriaList().stream().
                forEach(FilterApiDTO -> {
                    if (!checkIfValidComparisonOperator(FilterApiDTO.getExpType())) {
                        throw new IllegalArgumentException("Filter type does not match existing types");
                    }
                });
        if (StringUtils.isNotEmpty(inputDTO.getLogicalOperator())
                && !checkIfValidLogicalOperator(inputDTO.getLogicalOperator())) {
            throw new IllegalArgumentException("Logical Operator does not match existing types: 'AND', 'OR', 'XOR'");
        }
    }

}

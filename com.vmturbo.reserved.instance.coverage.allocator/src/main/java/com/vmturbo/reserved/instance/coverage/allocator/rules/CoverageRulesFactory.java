package com.vmturbo.reserved.instance.coverage.allocator.rules;

import static com.vmturbo.reserved.instance.coverage.allocator.rules.StaticCoverageRuleSet.ENTITY_MATCHER_CONFIGS_BY_CLOUD_PROVIDER;
import static com.vmturbo.reserved.instance.coverage.allocator.rules.StaticCoverageRuleSet.RULE_SET_BY_CLOUD_PROVIDER;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext.CloudServiceProvider;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.CoverageKey;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.CoverageEntityMatcher;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.CoverageEntityMatcher.CoverageEntityMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.EntityMatcherConfig;
import com.vmturbo.reserved.instance.coverage.allocator.rules.ConfigurableCoverageRule.ConfigurableCoverageRuleFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * A factory class for producing a list of {@link CoverageRule} instances, specific to a provided
 * cloud provider and entity/cloud commitment type.
 */
public class CoverageRulesFactory {

    private final Logger logger = LogManager.getLogger();

    private final ConfigurableCoverageRuleFactory configurableCoverageRuleFactory;

    private final CoverageEntityMatcherFactory coverageEntityMatcherFactory;

    /**
     * Constructs a new factory instance.
     * @param configurableCoverageRuleFactory A factory for producing {@link ConfigurableCoverageRule}
     *                                        instances.
     * @param coverageEntityMatcherFactory A factory for producing {@link CoverageEntityMatcher}
     *                                     instances.
     */
    public CoverageRulesFactory(@Nonnull ConfigurableCoverageRuleFactory configurableCoverageRuleFactory,
                                @Nonnull CoverageEntityMatcherFactory coverageEntityMatcherFactory) {
        this.configurableCoverageRuleFactory = Objects.requireNonNull(configurableCoverageRuleFactory);
        this.coverageEntityMatcherFactory = Objects.requireNonNull(coverageEntityMatcherFactory);
    }

    /**
     * Creates a set of coverage rules, corresponding to the coverage rules for the cloud provider.
     * The coverage rules are based on static configurations within {@link StaticCoverageRuleSet}.
     * The first rule will always be the {@link FirstPassCoverageRule}.
     * @param coverageContext The coverage context.
     * @param coverageJournal The coverage journal.
     * @return An immutable list of {@link CoverageRule} instances.
     */
    @Nonnull
    public List<CoverageRule> createRules(
            @Nonnull CloudProviderCoverageContext coverageContext,
            @Nonnull ReservedInstanceCoverageJournal coverageJournal) {

        final CloudServiceProvider csp = coverageContext.cloudServiceProvider();
        final SetMultimap<Long, CoverageKey> entityKeyMap = createEntityKeyMap(coverageContext);
        final List<CoverageRuleConfig> ruleConfigs = RULE_SET_BY_CLOUD_PROVIDER.get(csp);

        //Add the first pass coverage rules to the start of the list
        final ImmutableList.Builder rulesBuilder = ImmutableList.builder()
                .add(FirstPassCoverageRule.newInstance(coverageContext, coverageJournal));

        ruleConfigs.stream()
                .map(ruleConfig -> configurableCoverageRuleFactory.createRule(
                        coverageContext,
                        coverageJournal,
                        entityKeyMap,
                        ruleConfig))
                .forEach(rulesBuilder::add);

        return rulesBuilder.build();
    }

    private SetMultimap<Long, CoverageKey> createEntityKeyMap(@Nonnull CloudProviderCoverageContext coverageContext) {

        final CoverageTopology coverageTopology = coverageContext.coverageTopology();
        final CloudServiceProvider csp = coverageContext.cloudServiceProvider();
        if (ENTITY_MATCHER_CONFIGS_BY_CLOUD_PROVIDER.containsKey(csp)) {

            final Set<EntityMatcherConfig> entityMatcherConfigs = ENTITY_MATCHER_CONFIGS_BY_CLOUD_PROVIDER.get(csp);
            final ImmutableSetMultimap.Builder<Long, CoverageKey> entityKeySetBuilder = ImmutableSetMultimap.builder();

            final CoverageEntityMatcher entityMatcher = coverageEntityMatcherFactory.createEntityMatcher(
                    coverageTopology, entityMatcherConfigs);
            coverageContext.coverableEntityOids().forEach(entityOid ->
                    entityMatcher.createCoverageKeys(entityOid).forEach(coverageKey ->
                            entityKeySetBuilder.put(entityOid, coverageKey)));

            return entityKeySetBuilder.build();

        } else {
            logger.error("Cloud provider not found in static matcher configurations (Cloud Provider={})",
                    csp);
            return ImmutableSetMultimap.of();
        }
    }

}

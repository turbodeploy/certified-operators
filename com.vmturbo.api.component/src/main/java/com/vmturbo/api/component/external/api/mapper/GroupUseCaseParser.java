package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase.GroupUseCaseCriteria;

/**
 * Parse pre-defined groupBuilderUsecases.json file. This json file defines for each entity type
 * what are criteria rules for creating group. And these pre-defined criteria lists will be displayed
 * on UI when loading create group page.
 */
@Immutable
public class GroupUseCaseParser {
    private final Logger log = LogManager.getLogger();

    private final String useCasesFileName;

    private final Map<String, GroupUseCase> useCases;

    private final Map<String, GroupUseCaseCriteria> useCasesByFilterType;

    public GroupUseCaseParser(String useCasesFileName) {
        this.useCasesFileName = useCasesFileName;
        this.useCases = groupBuilderUseCases();
        this.useCasesByFilterType = computeUsesCaseByName();
    }

    /**
     * A class represent for all need criteria of entity type to create dynamic group.
     */
    @Immutable
    public static class GroupUseCase {
        private List<GroupUseCaseCriteria> criteria;

        /**
         * A class represent basic fields belong to criteria.
         */
        @Immutable
        public static class GroupUseCaseCriteria {
            private String inputType;
            private String elements;
            private String filterCategory;
            private String filterType;
            private boolean loadOptions;

            public GroupUseCaseCriteria(String inputType,
                                        String elements,
                                        String filterCategory,
                                        String filterType,
                                        boolean loadOptions) {
                this.inputType = inputType;
                this.elements = elements;
                this.filterCategory = filterCategory;
                this.filterType = filterType;
                this.loadOptions = loadOptions;
            }

            public String getInputType() {
                return this.inputType;
            }

            public String getElements() {
                return this.elements;
            }

            public String getFilterCategory() {
                return this.filterCategory;
            }

            public String getFilterType() {
                return this.filterType;
            }

            public boolean isLoadOptions() {
                return loadOptions;
            }

            @Override
            public String toString() {
                return getClass().getSimpleName() + "[" + filterType + "]";
            }
        }

        public GroupUseCase(List<GroupUseCaseCriteria> criteria) {
            this.criteria = ImmutableList.copyOf(criteria);
        }

        public List<GroupUseCaseCriteria> getCriteria() {
            return this.criteria;
        }

        @Override
        public String toString() {
            return criteria == null ? "<empty>" : criteria.toString();
        }
    }

    @Nonnull
    public Map<String, GroupUseCase> getUseCases() {
        return this.useCases;
    }

    @Nonnull
    public Map<String, GroupUseCaseCriteria> getUseCasesByFilterType() {
        return this.useCasesByFilterType;
    }

    /**
     * Read useCases json file and convert to Map object
     * @return a Map object represent group builder use cases
     */
    private Map<String, GroupUseCase> groupBuilderUseCases() {
        try (InputStream is = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream(useCasesFileName)) {
            log.info("Loading group builder use cases file " + useCasesFileName);
            String dataJSON = IOUtils.toString(is, Charset.defaultCharset());
            Type useCaseType = new TypeToken<Map<String, GroupUseCase>>() {}.getType();
            final Map<String, GroupUseCase> useCases = new Gson().fromJson(dataJSON, useCaseType);
            return Collections.unmodifiableMap(useCases);
        } catch (IOException e) {
            throw new RuntimeException("Unable to populate groupBUilderUsecases from file " + useCasesFileName);
        }
    }

    /**
     * Convert useCases Map to a Map which key is filterType and value is criteria map
     */
    private Map<String, GroupUseCaseCriteria> computeUsesCaseByName() {
        return Collections.unmodifiableMap(useCases.values()
                .stream()
                .map(GroupUseCase::getCriteria)
                .flatMap(List::stream)
                .collect(Collectors.toMap(GroupUseCaseCriteria::getFilterType,
                        Function.identity())));
    }
}

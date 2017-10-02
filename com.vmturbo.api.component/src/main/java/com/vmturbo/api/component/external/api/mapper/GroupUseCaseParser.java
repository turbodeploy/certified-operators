package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.vmturbo.api.component.external.api.mapper.GroupUseCaseParser.GroupUseCase.GroupUseCaseCriteria;

/**
 * Parse pre-defined groupBuilderUsecases.json file. This json file defines for each entity type
 * what are criteria rules for creating group. And these pre-defined criteria lists will be displayed
 * on UI when loading create group page.
 */
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
    public class GroupUseCase {
        private List<GroupUseCaseCriteria> criteria;

        /**
         * A class represent basic fields belong to criteria.
         */
        public class GroupUseCaseCriteria {
            private String inputType;
            private String elements;
            private String filterCategory;
            private String filterType;

            public GroupUseCaseCriteria(String inputType,
                                        String elements,
                                        String filterCategory,
                                        String filterType) {
                this.inputType = inputType;
                this.elements = elements;
                this.filterCategory = filterCategory;
                this.filterType = filterType;
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
        }

        public GroupUseCase(List<GroupUseCaseCriteria> criteria) {
            this.criteria = criteria;
        }

        public List<GroupUseCaseCriteria> getCriteria() {
            return this.criteria;
        }
    }

    public Map<String, GroupUseCase> getUseCases() {
        return this.useCases;
    }

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
            @SuppressWarnings("unchecked")
            Map<String, GroupUseCase> useCases = new Gson().fromJson(dataJSON, useCaseType);
            return useCases;
        } catch (IOException e) {
            throw new RuntimeException("Unable to populate groupBUilderUsecases from file " + useCasesFileName);
        }
    }

    /**
     * Convert useCases Map to a Map which key is filterType and value is criteria map
     */
    private Map<String, GroupUseCaseCriteria> computeUsesCaseByName() {
        return useCases.values().stream()
                .map(map -> map.getCriteria())
                .flatMap(List::stream)
                .collect(Collectors.toMap(map -> map.getFilterType(), Function.identity()));
    }
}

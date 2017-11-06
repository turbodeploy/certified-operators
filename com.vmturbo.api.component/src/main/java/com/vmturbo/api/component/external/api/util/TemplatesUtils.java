package com.vmturbo.api.component.external.api.util;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.mapper.TemplateMapper;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecsRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesByIdsRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc.TemplateSpecServiceBlockingStub;

/**
 * TemplatesUtils defined all allowed template field names which will be use to validate input
 * template information. And also defined helper functions related with fetching templates.
 */
public class TemplatesUtils {
    public static final String PROFILE = "Profile";
    // compute
    public static final String CPU_CONSUMED_FACTOR = "cpuConsumedFactor";
    public static final String CPU_SPEED = "cpuSpeed";
    public static final String IO_THROUGHPUT = "ioThroughput";
    public static final String IO_THROUGHPUT_SIZE = "ioThroughputSize";
    public static final String MEMORY_CONSUMED_FACTOR = "memoryConsumedFactor";
    public static final String MEMORY_SIZE = "memorySize";
    public static final String NUM_OF_CPU = "numOfCpu";
    public static final String NUM_OF_CORES = "numOfCores";
    public static final String NETWORK_THROUGHPUT = "networkThroughput";
    public static final String NETWORK_THROUGHPUT_SIZE = "networkThroughputSize";
    // storage
    public static final String DISK_IOPS = "diskIops";
    public static final String DISK_SIZE = "diskSize";
    public static final String DISK_CONSUMED_FACTOR = "diskConsumedFactor";

    public static Set<String> allowedComputeStats = Sets
        .newHashSet(NUM_OF_CPU, NUM_OF_CORES, CPU_SPEED, CPU_CONSUMED_FACTOR, MEMORY_SIZE,
            MEMORY_CONSUMED_FACTOR, IO_THROUGHPUT, IO_THROUGHPUT_SIZE,
            NETWORK_THROUGHPUT, NETWORK_THROUGHPUT_SIZE);
    public static Set<String> allowedStorageStats = Sets
        .newHashSet(DISK_SIZE, DISK_CONSUMED_FACTOR, DISK_IOPS);
    public static Set<String> allowedStorageTypes = Sets
        .newHashSet("disk", "rdm");

    private TemplateServiceBlockingStub templateService;

    private TemplateSpecServiceBlockingStub templateSpecService;

    private TemplateMapper templateMapper;

    public TemplatesUtils(@Nonnull final TemplateServiceBlockingStub templateService,
                          @Nonnull final TemplateSpecServiceBlockingStub templateSpecService,
                          @Nonnull final TemplateMapper templateMapper) {
        this.templateService = Objects.requireNonNull(templateService);
        this.templateSpecService = Objects.requireNonNull(templateSpecService);
        this.templateMapper = Objects.requireNonNull(templateMapper);
    }

    /**
     * Get a list of Templates based on input template ids, then convert Template to {@link TemplateApiDTO}
     * , and generate a Map which key is template id and value is {@link TemplateApiDTO}.
     *
     * @param templateIds set of template ids.
     * @return A Map which key is template id and value is {@link TemplateApiDTO}.
     */
    public Map<Long, TemplateApiDTO> getTemplatesMapByIds(@Nonnull final Set<Long> templateIds) {
        GetTemplatesByIdsRequest templatesByIdsRequest = GetTemplatesByIdsRequest.newBuilder()
            .addAllTemplateIds(templateIds)
            .build();
        Iterable<Template> templates = () -> templateService.getTemplatesByIds(templatesByIdsRequest);
        GetTemplateSpecsRequest templateSpecRequest = GetTemplateSpecsRequest.getDefaultInstance();
        Iterable<TemplateSpec> templateSpecs = () -> templateSpecService.getTemplateSpecs(templateSpecRequest);
        Map<Long, TemplateSpec> templateSpecMap = StreamSupport.stream(templateSpecs.spliterator(), false)
            .collect(Collectors.toMap(entry -> entry.getId(), Function.identity()));

        return StreamSupport.stream(templates.spliterator(), false)
            .collect(Collectors.toMap(Template::getId,
                template -> templateMapper.mapToTemplateApiDTO(template,
                    templateSpecMap.get(template.getTemplateInfo().getTemplateSpecId()))));
    }

    /**
     * Get a set of Templates based on input template ids. And if there are related templates, it will
     * return empty set.
     *
     * @param templateIds set of template ids.
     * @return set of Templates.
     */
    public Set<Template> getTemplatesByIds(@Nonnull final Set<Long> templateIds) {
        GetTemplatesByIdsRequest templatesByIdsRequest = GetTemplatesByIdsRequest.newBuilder()
            .addAllTemplateIds(templateIds)
            .build();
        Iterable<Template> templates = () -> templateService.getTemplatesByIds(templatesByIdsRequest);
        return StreamSupport.stream(templates.spliterator(), false)
            .collect(Collectors.toSet());
    }
}
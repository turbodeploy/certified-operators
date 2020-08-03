package com.vmturbo.api.component.external.api.util;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.mapper.TemplateMapper;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.common.protobuf.TemplateProtoUtil;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecsRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplatesRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.SingleTemplateResponse;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc.TemplateSpecServiceBlockingStub;

/**
 * TemplatesUtils defined all allowed template field names which will be use to validate input
 * template information. And also defined helper functions related with fetching templates.
 */
public class TemplatesUtils {
    public static final String PROFILE = "Profile";
    // compute
    public static final String MEMORY_SIZE = "memorySize";
    public static final String NUM_OF_CPU = "numOfCpu";
    private static final String CPU_CONSUMED_FACTOR = "cpuConsumedFactor";
    private static final String CPU_SPEED = "cpuSpeed";
    private static final String IO_THROUGHPUT = "ioThroughput";
    private static final String IO_THROUGHPUT_SIZE = "ioThroughputSize";
    private static final String MEMORY_CONSUMED_FACTOR = "memoryConsumedFactor";
    private static final String NUM_OF_CORES = "numOfCores";
    private static final String NETWORK_THROUGHPUT = "networkThroughput";
    private static final String NETWORK_THROUGHPUT_SIZE = "networkThroughputSize";
    // storage
    public static final String DISK_SIZE = "diskSize";
    private static final String DISK_IOPS = "diskIops";
    private static final String DISK_CONSUMED_FACTOR = "diskConsumedFactor";

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
        if (templateIds.isEmpty()) {
            return Collections.emptyMap();
        }

        return getTemplates(GetTemplatesRequest.newBuilder()
                .setFilter(TemplatesFilter.newBuilder()
                    .addAllTemplateIds(templateIds))
                .build())
            .collect(Collectors.toMap(apiDto -> Long.parseLong(apiDto.getUuid()), Function.identity()));
    }

    /**
     * Get all templates that match the provided request.
     *
     * @param templatesRequest The request to send to the template service.
     * @return A stream of the matching templates, converted to {@link TemplateApiDTO}s.
     */
    @Nonnull
    public Stream<TemplateApiDTO> getTemplates(@Nonnull final GetTemplatesRequest templatesRequest) {
        final GetTemplateSpecsRequest templateSpecRequest = GetTemplateSpecsRequest.getDefaultInstance();
        final Iterable<TemplateSpec> templateSpecs = () -> templateSpecService.getTemplateSpecs(templateSpecRequest);
        final Map<Long, TemplateSpec> templateSpecMap = StreamSupport.stream(templateSpecs.spliterator(), false)
            .collect(Collectors.toMap(TemplateSpec::getId, Function.identity()));

        return TemplateProtoUtil.flattenGetResponse(templateService.getTemplates(templatesRequest.toBuilder()
                // Ensure we include the deployment profiles in the returned data.
                .setIncludeDeploymentProfiles(true)
                .build()))
            .map(template -> {
                final TemplateApiDTO templateApiDTO = templateMapper.mapToTemplateApiDTO(
                    template.getTemplate(),
                    templateSpecMap.get(template.getTemplate().getTemplateInfo().getTemplateSpecId()),
                    template.getDeploymentProfileList());
                return templateApiDTO;
            });
    }

    /**
     * Get a set of Templates based on input template ids. And if there are related templates, it will
     * return empty set.
     *
     * @param templateIds set of template ids.
     * @return set of Templates.
     */
    public Set<Template> getTemplatesByIds(@Nonnull final Set<Long> templateIds) {
        if (templateIds.isEmpty()) {
            return Collections.emptySet();
        }
        GetTemplatesRequest templatesByIdsRequest = GetTemplatesRequest.newBuilder()
            .setFilter(TemplatesFilter.newBuilder()
                .addAllTemplateIds(templateIds))
            .build();
        return TemplateProtoUtil.flattenGetResponse(templateService.getTemplates(templatesByIdsRequest))
            .map(SingleTemplateResponse::getTemplate)
            .collect(Collectors.toSet());
    }
}
package com.vmturbo.mediation.azure.pricing.stages;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.PricingWorkspace;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline.Stage;
import com.vmturbo.mediation.azure.pricing.util.VmSizeParser;
import com.vmturbo.mediation.cost.common.CostContributersUtils;
import com.vmturbo.mediation.cost.parser.azure.AzureCostUtils;
import com.vmturbo.mediation.cost.parser.azure.VMSizes.VMSize;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.LicenseOverride;
import com.vmturbo.platform.sdk.common.PricingDTO.LicenseOverride.LicenseOverrideValue;
import com.vmturbo.platform.sdk.common.PricingDTO.LicenseOverrides;
import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;
import com.vmturbo.platform.sdk.probe.properties.PropertySpec;

/**
 * This stage constructs onDemandLicenseOverride and add it to the response.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind of
 *         discovery.
 */
public class LicenseOverridesStage<E extends ProbeStageEnum>
        extends Stage<PricingWorkspace, PricingWorkspace, E> {

    private static final Gson GSON = new GsonBuilder().create();

    private static final Type SET_OS_TYPE = new TypeToken<Set<OSType>>(){}.getType();
    private static final Function<String, Set<OSType>> OS_LIST_PARSER =
            s -> GSON.fromJson(s, SET_OS_TYPE);

    /**
     *  Set of OSTypes to be used to construct LicenceOverride.
     */
    public static final IProbePropertySpec<Set<OSType>> BASE_OS_SET = new PropertySpec<>(
            "licence_override_os_list", OS_LIST_PARSER,
            Sets.immutableEnumSet(OSType.LINUX, OSType.SUSE, OSType.RHEL, OSType.WINDOWS));
    private final VmSizeParser parser;
    private final IPropertyProvider propertyProvider;

    /**
     * Public constructor.
     *
     * @param probeStage the enum value representing this probe discovery stage, used for
     *         reporting detailed discovery status.
     * @param parser Parser Implementation to be used to fetch VmSize list.
     * @param propertyProvider properties configuring the discovery.
     */
    public LicenseOverridesStage(@Nonnull E probeStage, @Nonnull VmSizeParser parser,
            @Nonnull IPropertyProvider propertyProvider) {
        super(probeStage);
        this.parser = parser;
        this.propertyProvider = propertyProvider;
    }

    @NotNull
    @Override
    protected StageResult<PricingWorkspace> executeStage(@Nonnull PricingWorkspace pricingWorkspace)
            throws PipelineStageException {
        Set<OSType> baseOSSet = propertyProvider.getProperty(BASE_OS_SET);
        Map<String, LicenseOverrides> licenseOverrideMap = parser.getAllSizes().stream().filter(
                VMSize::hasBaseOsLicenseCoreOverride).collect(ImmutableMap.toImmutableMap(
                vmSize -> AzureCostUtils.getFormattedTemplateName(vmSize.getName(),
                        CostContributersUtils.TYPE_VM_TEMPLATE),
                vmSize -> LicenseOverrides.newBuilder()
                        .addAllLicenseOverride(baseOSSet.stream()
                                .map(osType -> LicenseOverride.newBuilder()
                                        .setOsType(osType)
                                        .setOverrideValue(LicenseOverrideValue.newBuilder()
                                                .setNumOfCores(
                                                        vmSize.getBaseOsLicenseCoreOverride())
                                                .build())
                                        .build())
                                .collect(ImmutableList.toImmutableList()))
                        .build()));
        pricingWorkspace.setOnDemandLicenseOverrides(licenseOverrideMap);
        String status = String.format("OnDemandLicenseOverrides added for %s Instance Types",
                licenseOverrideMap.size());
        getStageInfo().ok(status);
        return StageResult.withResult(pricingWorkspace).andStatus(Status.success(status));
    }
}

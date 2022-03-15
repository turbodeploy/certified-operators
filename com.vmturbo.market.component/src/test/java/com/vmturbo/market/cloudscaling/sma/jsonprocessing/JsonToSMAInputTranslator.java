package com.vmturbo.market.cloudscaling.sma.jsonprocessing;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.Gson;

import org.mockito.Mockito;

import com.vmturbo.cloud.common.commitment.CommitmentAmountCalculator;
import com.vmturbo.cloud.common.topology.SimulatedTopologyEntityCloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.SimulatedCloudCostData;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAMatchTestTrim;
import com.vmturbo.market.cloudscaling.sma.entities.CloudCostContextEntry;
import com.vmturbo.market.cloudscaling.sma.entities.SMACloudCostCalculator;
import com.vmturbo.market.cloudscaling.sma.entities.SMAConfig;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine;

/**
 * Read in Json and generate SMA data structures.
 */
public class JsonToSMAInputTranslator {

    private static final Gson GSON = ComponentGsonFactory.createGson();

    /**
     * construct the expected output from json file.
     * @param filename the json filename.
     * @param inputContext the corresponding input context.
     * @return list of matches.
     */
    public List<SMAMatch> readsmaOutput(String filename, SMAInputContext inputContext) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(filename));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        SMAMatchTestTrim[] output = GSON.fromJson(br, SMAMatchTestTrim[].class);
        List<SMAMatch> smaMatches = new ArrayList<>();
        for (SMAMatchTestTrim smaMatchTestTrim : output) {
            SMAVirtualMachine smaVirtualMachine = inputContext.getVirtualMachines().stream()
                    .filter(a -> a.getOid() == smaMatchTestTrim.getVirtualMachineOid()).findFirst().get();
            SMAReservedInstance smaReservedInstance = smaMatchTestTrim
                    .getReservedInstanceOid() == null ? null
                    : inputContext.getReservedInstances().stream()
                    .filter(a -> a.getOid() == smaMatchTestTrim.getReservedInstanceOid())
                    .findFirst().get();
            SMATemplate smaTemplate = inputContext.getTemplates().stream()
                    .filter(a -> a.getOid() == smaMatchTestTrim.getTemplateOid())
                    .findFirst().get();
            CloudCommitmentAmount discountedCoupons = smaMatchTestTrim.getReservedInstanceOid() == null ?
                    CommitmentAmountCalculator.ZERO_COVERAGE
                    : smaMatchTestTrim.getCloudCommitmentAmount() == null ?
                    CloudCommitmentAmount.newBuilder().setCoupons(smaMatchTestTrim.getDiscountedCoupons()).build()
                            : smaMatchTestTrim.getCloudCommitmentAmount();
            smaMatches.add(new SMAMatch(smaVirtualMachine, smaTemplate,
                    smaReservedInstance, discountedCoupons));
        }
        return smaMatches;
    }

    /**
     * construct the SMAInputContext from the json file.
     * @param filename json filename.
     * @return SMAInputContext constructed.
     */
    public SMAInput readsmaInput(String filename) {
        BufferedReader br = null;
        SMACloudCostCalculator cloudCostCalculator = new SMACloudCostCalculator(
                Mockito.mock(SimulatedTopologyEntityCloudTopology.class),

                Mockito.mock(SimulatedCloudCostData.class));
        try {
            br = new BufferedReader(new FileReader(filename));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        SMAInputContext smaInputContext = GSON.fromJson(br, SMAInputContext.class);
        if (smaInputContext.getSmaConfig() == null) {
            smaInputContext.setSmaConfig(new SMAConfig());
        }
        try {
            br = new BufferedReader(new FileReader(filename + ".cost"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        CloudCostContextEntry[] cloudCostContextEntries = GSON.fromJson(br, CloudCostContextEntry[].class);
        for(CloudCostContextEntry entry : cloudCostContextEntries) {
            cloudCostCalculator.getCloudCostLookUp().put(entry.getCostContext(), entry.getCostValue());
        }
        smaInputContext.decompress(cloudCostCalculator);
        // this will initialize fields which are not set in json.
        smaInputContext =  (new SMAInputContext(smaInputContext, cloudCostCalculator));
        SMAInput smaInput = new SMAInput(Collections.singletonList(smaInputContext), cloudCostCalculator);
        return smaInput;
    }

}

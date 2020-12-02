package com.vmturbo.market.cloudscaling.sma.jsonprocessing;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

import com.vmturbo.market.cloudscaling.sma.analysis.SMAMatchTestTrim;
import com.vmturbo.market.cloudscaling.sma.entities.SMAConfig;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine;

/**
 * Read in Json and generate SMA data structures.
 */
public class JsonToSMAInputTranslator {

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

        SMAMatchTestTrim[] output = new Gson().fromJson(br, SMAMatchTestTrim[].class);
        List<SMAMatch> smaMatches = new ArrayList<>();
        for (SMAMatchTestTrim smaMatchTestTrim : output) {
            SMAVirtualMachine smaVirtualMachine = inputContext.getVirtualMachines().stream()
                    .filter(a -> a.getOid() == smaMatchTestTrim.getVirtualMachineOid()).findFirst().get();
            if (smaMatchTestTrim.getReservedInstanceOid() == null) {

            }
            SMAReservedInstance smaReservedInstance = smaMatchTestTrim
                    .getReservedInstanceOid() == null ? null
                    : inputContext.getReservedInstances().stream()
                    .filter(a -> a.getOid() == smaMatchTestTrim.getReservedInstanceOid())
                    .findFirst().get();
            SMATemplate smaTemplate = inputContext.getTemplates().stream()
                    .filter(a -> a.getOid() == smaMatchTestTrim.getTemplateOid())
                    .findFirst().get();
            float discountedCoupons = smaMatchTestTrim.getReservedInstanceOid() == null ? 0f
                    : smaMatchTestTrim.getDiscountedCoupons();
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
    public SMAInputContext readsmaInput(String filename) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(filename));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        SMAInputContext smaInputContext = new Gson().fromJson(br, SMAInputContext.class);
        if (smaInputContext.getSmaConfig() == null) {
            smaInputContext.setSmaConfig(new SMAConfig());
        }
        smaInputContext.decompress();
        return smaInputContext;
    }

}

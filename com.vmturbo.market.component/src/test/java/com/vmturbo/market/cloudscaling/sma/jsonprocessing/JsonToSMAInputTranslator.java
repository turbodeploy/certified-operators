package com.vmturbo.market.cloudscaling.sma.jsonprocessing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.vmturbo.auth.api.Pair;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.cloudscaling.sma.entities.SMACSP;
import com.vmturbo.market.cloudscaling.sma.entities.SMAContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMACost;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Read in Json and generate SMA data structures.
 */
public class JsonToSMAInputTranslator {
    /**
     * parse the input json file.
     * @param filename input file
     * @return the corresponding SMAInput object
     */
    public SMAInput parseInput(String filename) {
        //JSON parser object to parse read file
        JsonParser jsonParser = new JsonParser();

        try (FileReader reader = new FileReader(filename)) {
            //Read JSON file
            Object obj = jsonParser.parse(reader);
            JsonObject input = (JsonObject)obj;
            List<SMAInputContext> inputContexts = new ArrayList<>();
            JsonArray inputContextsObjs = (JsonArray)input.get("Contexts");
            for (JsonElement contextElem : inputContextsObjs) {
                inputContexts.add(parseInputContext(contextElem.getAsJsonObject()));
            }
            SMAInput smaInput = new SMAInput(inputContexts);
            return smaInput;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

     /**
     * converts inputContext json object to SMAInputContext.
     * @param inputContextObj inputContext json object
     * @return corresponding SMAInputContext
     */
    private SMAInputContext parseInputContext(JsonObject inputContextObj) {
        Map<Long, SMATemplate> oidToTemplate = new HashMap<>();
        List<SMATemplate> templates = new ArrayList<>();
        JsonArray templatesObjs = (JsonArray)inputContextObj.get("Templates");
        for (JsonElement templatesObj : templatesObjs) {
            SMATemplate template = parseTemplate(templatesObj.getAsJsonObject());
            templates.add(template);
            oidToTemplate.put(template.getOid(), template);
        }

        JsonObject contextObj = (JsonObject)inputContextObj.get("context");
        SMAContext context = parseContext(contextObj);

        Map<Long, SMAReservedInstance> oidToRI = new HashMap<>();
        List<SMAReservedInstance> reservedInstances = new ArrayList<>();
        JsonArray reservedInstancesObjs = (JsonArray)inputContextObj.get("ReservedInstances");
        if (reservedInstancesObjs != null) {
            for (JsonElement reservedInstancesObj : reservedInstancesObjs) {
                SMAReservedInstance reservedInstance = parseReservedInstance(reservedInstancesObj.getAsJsonObject(), oidToTemplate, context);
                reservedInstances.add(reservedInstance);
                oidToRI.put(reservedInstance.getRiKeyOid(), reservedInstance);
            }
        }

        List<SMAVirtualMachine> virtualMachines = new ArrayList<>();
        JsonArray virtualMachinesObjs = (JsonArray)inputContextObj.get("VirtualMachines");
        for (JsonElement virtualMachinesObj : virtualMachinesObjs) {
            List<SMAVirtualMachine> smaVirtualMachines = parseVirtualMachine(virtualMachinesObj.getAsJsonObject(),
                oidToTemplate, oidToRI);
            virtualMachines.addAll(smaVirtualMachines);
        }


        SMAInputContext inputContext = new SMAInputContext(context, virtualMachines, reservedInstances, templates);
        return inputContext;

    }

    private SMAReservedInstance parseReservedInstance(JsonObject reservedInstanceObj,
                                                      Map<Long, SMATemplate> oidToTemplate, SMAContext context) {
        long oid = reservedInstanceObj.get("oid").getAsLong();
        long keyOid = reservedInstanceObj.get("keyOid").getAsLong();
        String name = reservedInstanceObj.get("name").getAsString();
        long businessAccount = reservedInstanceObj.get("businessAccount").getAsLong();
        long templateOid = reservedInstanceObj.get("template").getAsLong();
        int count = reservedInstanceObj.get("count").getAsInt();
        boolean isf = reservedInstanceObj.get("isf").getAsBoolean();
        boolean shared = true;
        if (reservedInstanceObj.has("shared")) {
            shared = reservedInstanceObj.get("shared").getAsBoolean();
        }
        Set<Long> applicableBusinessAccounts = Collections.emptySet();
        if (reservedInstanceObj.has("applicableBusinessAccounts")) {
            JsonArray jsonArray = reservedInstanceObj.get("applicableBusinessAccounts").getAsJsonArray();
            Long[] arrName = new Gson().fromJson(jsonArray, Long[].class);
            applicableBusinessAccounts = ImmutableSet.copyOf(arrName);
        }
        boolean platformFlexible = false;
        if (reservedInstanceObj.has("platformFlexible")) {
            reservedInstanceObj.get("platformFlexible").getAsBoolean();
        }
        long zone = SMAUtils.NO_ZONE;
        JsonElement zoneObj = reservedInstanceObj.get("zone");
        if (zoneObj != null) {
            zone = reservedInstanceObj.get("zone").getAsLong();
        }
        SMAReservedInstance reservedInstance = new SMAReservedInstance(oid, keyOid, name,
                businessAccount, applicableBusinessAccounts, oidToTemplate.get(templateOid), zone,
                count, isf, shared, platformFlexible);
        return reservedInstance;
    }

    private SMATemplate parseTemplate(JsonObject templateObj) {
        Long oid = templateObj.get("oid").getAsLong();
        String name = templateObj.get("name").getAsString();
        String family = templateObj.get("family").getAsString();
        int coupons = templateObj.get("coupons").getAsInt();
        SMATemplate template = new SMATemplate(oid, name, family, coupons, SMAUtils.BOGUS_CONTEXT, null);

        JsonArray onDemandCostObjs = (JsonArray)templateObj.get("onDemandCost");
        for (JsonElement onDemandCostObj : onDemandCostObjs) {
            SMACost onDemandCost = new SMACost(
                    onDemandCostObj.getAsJsonObject().get("compute").getAsFloat(),
                    onDemandCostObj.getAsJsonObject().get("license").getAsFloat());
            template.setOnDemandCost(
                    onDemandCostObj.getAsJsonObject().get("businessAccount").getAsLong(),
                    OSType.valueOf(onDemandCostObj.getAsJsonObject().get("os").getAsString()),
                    onDemandCost);
        }
        JsonArray discountedCostObjs = (JsonArray)templateObj.get("discountedCost");
        for (JsonElement discountedCostObj : discountedCostObjs) {
            SMACost discountedCost = new SMACost(
                    discountedCostObj.getAsJsonObject().get("compute").getAsFloat(),
                    discountedCostObj.getAsJsonObject().get("license").getAsFloat());
            template.setDiscountedCost(
                    discountedCostObj.getAsJsonObject().get("businessAccount").getAsLong(),
                OSType.valueOf(discountedCostObj.getAsJsonObject().get("os").getAsString()),
                    discountedCost);
        }
        return template;
    }

    private List<SMAVirtualMachine>   parseVirtualMachine(JsonObject virtualMachineObj,
                                                          Map<Long, SMATemplate> oidToTemplate,
                                                          Map<Long, SMAReservedInstance> oidToRI) {
        Long oid = virtualMachineObj.get("oid").getAsLong();
        String name = virtualMachineObj.get("name").getAsString();
        Long businessAccountOid = virtualMachineObj.get("businessAccount").getAsLong();
        OSType osType = OSType.valueOf(virtualMachineObj.get("os").getAsString());
        float currentRICoverage = 0f;
        if (virtualMachineObj.has("currentRICoverage")) {
            currentRICoverage = virtualMachineObj.get("currentRICoverage").getAsFloat();
        }
        JsonElement currentRIObj = virtualMachineObj.get("currentRIKeyID");
        long currentRiOid = SMAUtils.UNKNOWN_OID;
        if (currentRIObj != null) {
            currentRiOid = currentRIObj.getAsLong();
        }
        Long currentTemplateOid = virtualMachineObj.get("currentTemplate").getAsLong();
        List<SMATemplate> providers = new ArrayList<>();
        JsonArray providersObjs = (JsonArray)virtualMachineObj.get("providers");
        for (JsonElement providersObj : providersObjs) {
            providers.add(oidToTemplate.get(providersObj.getAsLong()));
        }
        Long zoneOid = virtualMachineObj.get("zone").getAsLong();
        JsonElement groupNameObj = virtualMachineObj.get("groupName");
        String groupName = SMAUtils.NO_GROUP_ID;
        if (groupNameObj != null) {
            groupName = groupNameObj.getAsString();
        }
        JsonElement countObj = virtualMachineObj.get("count");
        List<SMAVirtualMachine> smaVirtualMachines = new ArrayList<>();
        if (countObj != null) {
            int count = countObj.getAsInt();
            for (Integer i = 0; i < count; i++) {
                String vm_name = name + i.toString();
                long newOid = oid + i;
                SMAVirtualMachine virtualMachine = new SMAVirtualMachine(newOid, vm_name, groupName, businessAccountOid,
                    oidToTemplate.get(currentTemplateOid), providers, currentRICoverage, zoneOid,
                    oidToRI.get(currentRiOid), osType);
                smaVirtualMachines.add(virtualMachine);
            }
        } else {
            SMAVirtualMachine virtualMachine = new SMAVirtualMachine(oid, name, groupName, businessAccountOid,
                oidToTemplate.get(currentTemplateOid), providers, currentRICoverage, zoneOid,
                oidToRI.get(currentRiOid), osType);
            smaVirtualMachines.add(virtualMachine);
        }
        return smaVirtualMachines;
    }

    private SMAContext parseContext(JsonObject contextObj) {
        JsonElement cspObj = contextObj.get("csp");
        SMACSP csp = SMACSP.valueOf(cspObj.getAsString());
        OSType os = OSType.valueOf(contextObj.get("os").getAsString());
        long region = contextObj.get("region").getAsLong();
        long billingAccount = contextObj.get("billingAccount").getAsLong();
        Tenancy tenancy = Tenancy.valueOf(contextObj.get("tenancy").getAsString());
        SMAContext context = new SMAContext(csp, os, region, billingAccount, tenancy);
        return context;
    }

    private SMAMatch parseMatch(JsonObject matchesObj, SMAInputContext smaInputContext) {
        long virtualMachineOid = matchesObj.get("VirtualMachine").getAsLong();
        long templateOid = matchesObj.get("Template").getAsLong();
        JsonElement reservedInstanceObj = matchesObj.get("ReservedInstance");
        int discountedCoupons = 0;
        SMAReservedInstance reservedInstance = null;
        if (reservedInstanceObj != null) {
            long reservedInstanceOid = reservedInstanceObj.getAsLong();
            discountedCoupons = matchesObj.get("DiscountedCoupons").getAsInt();
            for (SMAReservedInstance ri : smaInputContext.getReservedInstances()) {
                if (ri.getOid() == reservedInstanceOid) {
                    reservedInstance = ri;
                    break;
                }
            }
            Objects.requireNonNull(reservedInstance, "parseMatch: RI == null, could not RI=" +
                reservedInstanceOid + " in inputContext");
        }

        SMAVirtualMachine virtualMachine = null;
        for (SMAVirtualMachine vm : smaInputContext.getVirtualMachines()) {
            if (vm.getOid() == virtualMachineOid) {
                virtualMachine = vm;
                break;
            }
        }
        Objects.requireNonNull(virtualMachine, "parseMatch: VM == null for OID=" + virtualMachineOid +
            ", could not  VM in input context");


        SMATemplate template = null;
        for (SMATemplate tem : smaInputContext.getTemplates()) {
            if (tem.getOid() == templateOid) {
                template = tem;
                break;
            }
        }
        Objects.requireNonNull(template, "parseMatch: template == null for OID=" + templateOid +
            ", could not find template in input context");


        SMAMatch smaMatch = new SMAMatch(virtualMachine, template, reservedInstance, discountedCoupons);
        return smaMatch;

    }

    private SMAOutputContext parseOutputContext(JsonObject outputContextObj, SMAInputContext smaInputContext) {
        SMAContext context = smaInputContext.getContext();
        JsonArray matchesObj = (JsonArray)outputContextObj.get("Matching");
        List<SMAMatch> matches = new ArrayList<>();
        for (JsonElement matchingObj : matchesObj) {
            SMAMatch match = parseMatch(matchingObj.getAsJsonObject(), smaInputContext);
            matches.add(match);
        }
        SMAOutputContext outputContext = new SMAOutputContext(context, matches);
        return outputContext;
    }

    /**
     * parse the json file that has the input scenario and expected output.
     * @param filename json input file.
     * @return SMAInput object and the expected SMAOutput object parsed from file
     */
    public com.vmturbo.auth.api.Pair<SMAInput, SMAOutput> parseInputWithExpectedOutput(String filename) {
        //JSON parser object to parse read file
        JsonParser jsonParser = new JsonParser();

        try (FileReader reader = new FileReader(filename)) {
            Object obj = jsonParser.parse(reader);

            JsonObject input = (JsonObject)obj;
            //Read JSON file
            List<SMAInputContext> inputContexts = new ArrayList<>();
            List<SMAOutputContext> outputContexts = new ArrayList<>();
            JsonArray inputContextsObjs = (JsonArray)input.get("Contexts");
            for (JsonElement contextElem : inputContextsObjs) {
                SMAInputContext smaInputContext = parseInputContext(contextElem.getAsJsonObject());
                inputContexts.add(smaInputContext);
                SMAOutputContext smaOutputContext = parseOutputContext(contextElem.getAsJsonObject(), smaInputContext);
                outputContexts.add(smaOutputContext);
            }
            SMAInput smaInput = new SMAInput(inputContexts);
            SMAOutput smaOutput = new SMAOutput(outputContexts);
            return new Pair<>(smaInput, smaOutput);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}

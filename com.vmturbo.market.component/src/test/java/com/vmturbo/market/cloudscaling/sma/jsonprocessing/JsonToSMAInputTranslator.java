package com.vmturbo.market.cloudscaling.sma.jsonprocessing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

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
     * convert the given SMAInput and SMAOutput to a json file.
     * @param smaInput the smaInput to convert
     * @param smaOutput the smaOutput to convert
     * @param filename the filename of the json file.
     */
    public void convertSmaToJson(SMAInput smaInput, SMAOutput smaOutput, String filename) {
        File file = new File(filename);
        FileWriter fr = null;
        BufferedWriter br = null;
        try {
            fr = new FileWriter(file);
            br = new BufferedWriter(fr);
            br.write("{\n");
            br.write("\t\"Contexts\": [\n");
            br.write("\t\t{\n");
            for (int contextIndex = 0; contextIndex < smaInput.getContexts().size(); contextIndex++) {
                SMAInputContext smaInputContext = smaInput.getContexts().get(contextIndex);
                br.write("\t\t\t\"context\": {\n");
                SMAContext smaContext = smaInputContext.getContext();
                br.write("\t\t\t\t\"os\": \"" + smaContext.getOs().name() + "\",\n");
                br.write("\t\t\t\t\"csp\": \"" + smaContext.getCsp().name() + "\",\n");
                br.write("\t\t\t\t\"region\": " + smaContext.getRegionId() + ",\n");
                br.write("\t\t\t\t\"billingAccount\": " + smaContext.getBillingAccountId() + ",\n");
                br.write("\t\t\t\t\"tenancy\": \"" + smaContext.getTenancy().name() + "\"\n");
                br.write("\t\t\t},\n");

                br.write("\t\t\t\"Templates\": [\n");
                for (SMATemplate smaTemplate : smaInputContext.getTemplates()) {
                    br.write("\t\t\t\t{\n");
                    br.write("\t\t\t\t\t\"oid\": " + smaTemplate.getOid() + ",\n");
                    br.write("\t\t\t\t\t\"name\": \"" + smaTemplate.getName() + "\",\n");
                    br.write("\t\t\t\t\t\"family\": \"" + smaTemplate.getFamily() + "\",\n");

                    br.write("\t\t\t\t\t\"onDemandCost\": [\n");
                    int currentIndex = 0;
                    for (Entry<Long, SMACost> entry : smaTemplate.getOnDemandCosts().entrySet()) {
                        br.write("\t\t\t\t\t\t{\n");
                        br.write("\t\t\t\t\t\t\t\"compute\": " + entry.getValue().getCompute() + ",\n");
                        br.write("\t\t\t\t\t\t\t\"license\": " + entry.getValue().getLicense() + ",\n");
                        br.write("\t\t\t\t\t\t\t\"businessAccount\": " + entry.getKey() + "\n");

                        if (currentIndex != smaTemplate.getOnDemandCosts().values().size() - 1) {
                            br.write("\t\t\t\t\t\t},\n");
                        }
                        currentIndex++;
                    }
                    br.write("\t\t\t\t\t\t}\n");
                    br.write("\t\t\t\t\t],\n");

                    br.write("\t\t\t\t\t\"discountedCost\": [\n");
                    currentIndex = 0;
                    for (Entry<Long, SMACost> entry : smaTemplate.getDiscountedCosts().entrySet()) {
                        br.write("\t\t\t\t\t\t{\n");
                        br.write("\t\t\t\t\t\t\t\"compute\": " + entry.getValue().getCompute() + ",\n");
                        br.write("\t\t\t\t\t\t\t\"license\": " + entry.getValue().getLicense() + ",\n");
                        br.write("\t\t\t\t\t\t\t\"businessAccount\": " + entry.getKey() + "\n");

                        if (currentIndex != smaTemplate.getDiscountedCosts().values().size() - 1) {
                            br.write("\t\t\t\t\t\t},\n");
                        }
                        currentIndex++;
                    }
                    br.write("\t\t\t\t\t\t}\n");
                    br.write("\t\t\t\t\t],\n");

                    br.write("\t\t\t\t\t\"coupons\": " + smaTemplate.getCoupons() + "\n");
                    if (smaTemplate != smaInputContext.getTemplates().get(smaInputContext.getTemplates().size() - 1)) {
                        br.write("\t\t\t\t},\n");
                    }
                }
                br.write("\t\t\t\t}\n");
                br.write("\t\t\t],\n");

                br.write("\t\t\t\"VirtualMachines\": [\n");
                for (SMAVirtualMachine smaVirtualMachine : smaInputContext.getVirtualMachines()) {
                    br.write("\t\t\t\t{\n");
                    br.write("\t\t\t\t\t\"oid\": " + smaVirtualMachine.getOid() + ",\n");
                    br.write("\t\t\t\t\t\"name\": \"" + smaVirtualMachine.getName() + "\",\n");
                    br.write("\t\t\t\t\t\"businessAccount\": " + smaVirtualMachine.getBusinessAccount() + ",\n");
                    br.write("\t\t\t\t\t\"providers\": [\n");
                    for (SMATemplate provider : smaVirtualMachine.getProviders()) {
                        br.write("\t\t\t\t\t\t" + provider.getOid());
                        if (provider != smaVirtualMachine.getProviders().get(smaVirtualMachine.getProviders().size() - 1)) {
                            br.write(",\n");
                        }
                    }
                    br.write("\n");
                    br.write("\t\t\t\t\t],\n");
                    br.write("\t\t\t\t\t\"zone\": " + smaVirtualMachine.getZone() + ",\n");
                    if (!smaVirtualMachine.getGroupName().equals(SMAUtils.NO_GROUP_ID)) {
                        br.write("\t\t\t\t\t\"groupName\": " + smaVirtualMachine.getGroupName() + ",\n");
                    }
                    br.write("\t\t\t\t\t\"currentRICoverage\": " + Math.round(smaVirtualMachine.getCurrentRICoverage()) + ",\n");
                    br.write("\t\t\t\t\t\"currentRIKeyID\": " + Math.round(smaVirtualMachine.getCurrentRIKey()) + ",\n");
                    br.write("\t\t\t\t\t\"currentTemplate\": " + smaVirtualMachine.getCurrentTemplate().getOid() + "\n");

                    if (smaVirtualMachine != smaInputContext.getVirtualMachines().get(smaInputContext.getVirtualMachines().size() - 1)) {
                        br.write("\t\t\t\t},\n");
                    }
                }
                br.write("\t\t\t\t}\n");
                br.write("\t\t\t],\n");

                br.write("\t\t\t\"ReservedInstances\": [\n");
                for (SMAReservedInstance smaReservedInstance : smaInputContext.getReservedInstances()) {
                    br.write("\t\t\t\t{\n");
                    br.write("\t\t\t\t\t\"oid\": " + smaReservedInstance.getOid() + ",\n");
                    br.write("\t\t\t\t\t\"keyOid\": " + smaReservedInstance.getRiKeyOid() + ",\n");
                    br.write("\t\t\t\t\t\"name\": \"" + smaReservedInstance.getName() + "\",\n");
                    br.write("\t\t\t\t\t\"businessAccount\": " + smaReservedInstance.getBusinessAccount() + ",\n");
                    br.write("\t\t\t\t\t\"utilization\": " + "0,\n");
                    br.write("\t\t\t\t\t\"zone\": " + smaReservedInstance.getZone() + ",\n");
                    br.write("\t\t\t\t\t\"template\": " + smaReservedInstance.getTemplate().getOid() + ",\n");
                    br.write("\t\t\t\t\t\"count\": " + smaReservedInstance.getCount() + ",\n");
                    br.write("\t\t\t\t\t\"isf\": " + smaReservedInstance.isIsf() + "\n");
                    if (smaReservedInstance != smaInputContext.getReservedInstances().get(smaInputContext.getReservedInstances().size() - 1)) {
                        br.write("\t\t\t\t},\n");
                    }
                }
                if (smaInputContext.getReservedInstances().size() > 0) {
                    br.write("\t\t\t\t}\n");
                }
                if (smaOutput != null && smaOutput.getContexts().size() == smaInput.getContexts().size()) {
                    br.write("\t\t\t],\n");
                } else {
                    br.write("\t\t\t]\n");
                }

                if (smaOutput != null && smaOutput.getContexts().size() == smaInput.getContexts().size()) {
                    SMAOutputContext smaOutputContext = smaOutput.getContexts().get(contextIndex);
                    br.write("\t\t\t\"Matching\": [\n");
                    for (SMAMatch smaMatch : smaOutputContext.getMatches()) {
                        br.write("\t\t\t\t{\n");
                        br.write("\t\t\t\t\t\"VirtualMachine\": " + smaMatch.getVirtualMachine().getOid() + ",\n");
                        if (smaMatch.getDiscountedCoupons() > 0) {
                            br.write("\t\t\t\t\t\"ReservedInstance\": " + smaMatch.getReservedInstance().getOid() + ",\n");
                            br.write("\t\t\t\t\t\"DiscountedCoupons\": " + smaMatch.getDiscountedCoupons() + ",\n");
                        }
                        br.write("\t\t\t\t\t\"Template\": " + smaMatch.getTemplate().getOid() + "\n");
                        if (smaMatch != smaOutputContext.getMatches().get(smaOutputContext.getMatches().size() - 1)) {
                            br.write("\t\t\t\t},\n");
                        }
                    }
                    br.write("\t\t\t\t}\n");
                    br.write("\t\t\t]\n");
                }

                if (smaInputContext != smaInput.getContexts().get(smaInput.getContexts().size() - 1)) {
                    br.write("\t\t},\n");
                }
            }
            br.write("\t\t}\n");
            br.write("\t]\n");
            br.write("}\n");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
                fr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
        List<SMAVirtualMachine> virtualMachines = new ArrayList<>();
        JsonArray virtualMachinesObjs = (JsonArray)inputContextObj.get("VirtualMachines");
        for (JsonElement virtualMachinesObj : virtualMachinesObjs) {
            List<SMAVirtualMachine> smaVirtualMachines = parseVirtualMachine(virtualMachinesObj.getAsJsonObject(), oidToTemplate);
            virtualMachines.addAll(smaVirtualMachines);
        }

        JsonObject contextObj = (JsonObject)inputContextObj.get("context");
        SMAContext context = parseContext(contextObj);

        List<SMAReservedInstance> reservedInstances = new ArrayList<>();
        JsonArray reservedInstancesObjs = (JsonArray)inputContextObj.get("ReservedInstances");
        if (reservedInstancesObjs != null) {
            for (JsonElement reservedInstancesObj : reservedInstancesObjs) {
                SMAReservedInstance reservedInstance = parseReservedInstance(reservedInstancesObj.getAsJsonObject(), oidToTemplate, context);
                reservedInstances.add(reservedInstance);
            }
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
        boolean isf = reservedInstanceObj.get("isf").getAsBoolean();
        long zone = SMAUtils.NO_ZONE;
        JsonElement zoneObj = reservedInstanceObj.get("zone");
        if (zoneObj != null) {
            zone = reservedInstanceObj.get("zone").getAsLong();
        }
        int count = reservedInstanceObj.get("count").getAsInt();
        SMAReservedInstance reservedInstance = new SMAReservedInstance(oid, keyOid, name, businessAccount,
                oidToTemplate.get(templateOid), zone, count, isf);
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
                    onDemandCost);
        }
        JsonArray discountedCostObjs = (JsonArray)templateObj.get("discountedCost");
        for (JsonElement discountedCostObj : discountedCostObjs) {
            SMACost discountedCost = new SMACost(
                    discountedCostObj.getAsJsonObject().get("compute").getAsFloat(),
                    discountedCostObj.getAsJsonObject().get("license").getAsFloat());
            template.setDiscountedCost(
                    discountedCostObj.getAsJsonObject().get("businessAccount").getAsLong(),
                    discountedCost);
        }
        return template;
    }

    private List<SMAVirtualMachine>   parseVirtualMachine(JsonObject virtualMachineObj, Map<Long, SMATemplate> oidToTemplate) {
        Long oid = virtualMachineObj.get("oid").getAsLong();
        String name = virtualMachineObj.get("name").getAsString();
        Long businessAccountOid = virtualMachineObj.get("businessAccount").getAsLong();
        float currentRICoverage = virtualMachineObj.get("currentRICoverage").getAsFloat();
        JsonElement currentRIObj = virtualMachineObj.get("currentRIKeyID");
        long currentRI = SMAUtils.NO_CURRENT_RI;
        if (currentRIObj != null) {
            currentRI = currentRIObj.getAsLong();
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
                    oidToTemplate.get(currentTemplateOid), providers, currentRICoverage, zoneOid, currentRI);
                virtualMachine.updateNaturalTemplateAndMinCostProviderPerFamily();
                smaVirtualMachines.add(virtualMachine);
            }
        } else {
            SMAVirtualMachine virtualMachine = new SMAVirtualMachine(oid, name, groupName, businessAccountOid,
                oidToTemplate.get(currentTemplateOid), providers, currentRICoverage, zoneOid, currentRI);
            virtualMachine.updateNaturalTemplateAndMinCostProviderPerFamily();
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

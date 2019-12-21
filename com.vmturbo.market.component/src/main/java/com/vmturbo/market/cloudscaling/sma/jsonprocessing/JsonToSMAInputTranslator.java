package com.vmturbo.market.cloudscaling.sma.jsonprocessing;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.vmturbo.market.cloudscaling.sma.entities.SMAPlatform;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.market.cloudscaling.sma.entities.SMATenancy;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine;

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
        for (JsonElement reservedInstancesObj : reservedInstancesObjs) {
            SMAReservedInstance reservedInstance = parseReservedInstance(reservedInstancesObj.getAsJsonObject(), oidToTemplate, context);
            reservedInstances.add(reservedInstance);
        }

        SMAInputContext inputContext = new SMAInputContext(context, virtualMachines, reservedInstances, templates);
        return inputContext;

    }

    private SMAReservedInstance parseReservedInstance(JsonObject reservedInstanceObj,
                                                      Map<Long, SMATemplate> oidToTemplate, SMAContext context) {
        long oid = reservedInstanceObj.get("oid").getAsLong();
        String name = reservedInstanceObj.get("name").getAsString();
        long businessAccount = reservedInstanceObj.get("businessAccount").getAsLong();
        float utilization = reservedInstanceObj.get("utilization").getAsFloat();
        long templateOid = reservedInstanceObj.get("template").getAsLong();
        long zone = SMAUtils.NO_ZONE;
        JsonElement zoneObj = reservedInstanceObj.get("zone");
        if (zoneObj != null) {
            zone = reservedInstanceObj.get("zone").getAsLong();
        }
        int count = reservedInstanceObj.get("count").getAsInt();
        SMAReservedInstance reservedInstance = new SMAReservedInstance(oid, name, businessAccount, utilization, oidToTemplate.get(templateOid), zone, count, context);
        return reservedInstance;
    }

    private SMATemplate parseTemplate(JsonObject templateObj) {
        Long oid = templateObj.get("oid").getAsLong();
        String name = templateObj.get("name").getAsString();
        String family = templateObj.get("family").getAsString();
        JsonObject onDemandCostObj = (JsonObject)templateObj.get("onDemandCost");
        JsonObject discountedCostObj = (JsonObject)templateObj.get("discountedCost");
        SMACost onDemandCost = new SMACost(onDemandCostObj.get("compute").getAsFloat(), onDemandCostObj.get("license").getAsFloat());
        SMACost discountedCost = new SMACost(discountedCostObj.get("compute").getAsFloat(), discountedCostObj.get("license").getAsFloat());
        int coupons = templateObj.get("coupons").getAsInt();
        SMATemplate template = new SMATemplate(oid, name, family, onDemandCost, discountedCost, coupons);
        return template;
    }

    private List<SMAVirtualMachine>   parseVirtualMachine(JsonObject virtualMachineObj, Map<Long, SMATemplate> oidToTemplate) {
        Long oid = virtualMachineObj.get("oid").getAsLong();
        String name = virtualMachineObj.get("name").getAsString();
        Long businessAccountOid = virtualMachineObj.get("businessAccount").getAsLong();
        float currentRICoverage = virtualMachineObj.get("currentRICoverage").getAsFloat();
        Long currentTemplateOid = virtualMachineObj.get("currentTemplate").getAsLong();
        List<SMATemplate> providers = new ArrayList<>();
        JsonArray providersObjs = (JsonArray)virtualMachineObj.get("providers");
        for (JsonElement providersObj : providersObjs) {
            providers.add(oidToTemplate.get(providersObj.getAsLong()));
        }
        Long zoneOid = virtualMachineObj.get("zone").getAsLong();
        JsonElement groupNameObj = virtualMachineObj.get("groupName");
        Long groupOid = SMAUtils.NO_GROUP_OID;
        if (groupNameObj != null) {
            groupOid = groupNameObj.getAsLong();
        }
        JsonElement countObj = virtualMachineObj.get("count");
        List<SMAVirtualMachine> smaVirtualMachines = new ArrayList<>();
        if (countObj != null) {
            int count = countObj.getAsInt();
            for (Integer i = 0; i < count; i++) {
                String vm_name = name + i.toString();
                long newOid = oid + i;
                SMAVirtualMachine virtualMachine = new SMAVirtualMachine(newOid, vm_name, groupOid, businessAccountOid,
                    oidToTemplate.get(currentTemplateOid), providers, currentRICoverage, zoneOid);
                smaVirtualMachines.add(virtualMachine);
            }
        } else {
            SMAVirtualMachine virtualMachine = new SMAVirtualMachine(oid, name, groupOid, businessAccountOid,
                oidToTemplate.get(currentTemplateOid), providers, currentRICoverage, zoneOid);
            smaVirtualMachines.add(virtualMachine);
        }
        return smaVirtualMachines;
    }

    private SMAContext parseContext(JsonObject contextObj) {
        SMACSP csp = SMACSP.valueOf(contextObj.get("csp").getAsString());
        SMAPlatform os = SMAPlatform.valueOf(contextObj.get("os").getAsString());
        long region = contextObj.get("region").getAsLong();
        long billingAccount = contextObj.get("billingAccount").getAsLong();
        SMATenancy tenancy = SMATenancy.valueOf(contextObj.get("tenancy").getAsString());
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

        JsonArray reservedInstancesObj = (JsonArray)matchesObj.get("ReservedInstances");
        List<Pair<SMAReservedInstance, Integer>> memberReservedInstances = new ArrayList<>();
        if (reservedInstancesObj != null) {
            for (JsonElement rInstanceObj : reservedInstancesObj) {
                long reservedInstanceOid = rInstanceObj.getAsJsonObject().get("ReservedInstance").getAsLong();
                int memberdiscountedCoupons = rInstanceObj.getAsJsonObject().get("DiscountedCoupons").getAsInt();
                SMAReservedInstance memberReservedInstance = null;
                for (SMAReservedInstance ri : smaInputContext.getReservedInstances()) {
                    if (ri.getOid() == reservedInstanceOid) {
                        memberReservedInstance = ri;
                        break;
                    }
                }
                memberReservedInstances.add(new Pair<>(memberReservedInstance, memberdiscountedCoupons));
            }
        }
        if (memberReservedInstances.isEmpty()) {
            SMAMatch smaMatch = new SMAMatch(virtualMachine, template, reservedInstance, discountedCoupons);
            return smaMatch;
        } else {
            discountedCoupons = matchesObj.get("DiscountedCoupons").getAsInt();
            SMAMatch smaMatch = new SMAMatch(virtualMachine, template, discountedCoupons, memberReservedInstances);
            return smaMatch;
        }


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

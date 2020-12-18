package com.vmturbo.topology.processor.identity.extractor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.commons.extractor.VMTExtractorException;
import com.vmturbo.commons.extractor.impl.Property;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.topology.processor.identity.EntityDescriptor;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityIdentityMetadata;
import com.vmturbo.topology.processor.identity.metadata.ServiceEntityProperty;

/**
 * Extracts identifying properties from EntityDTO.
 */
public class IdentifyingPropertyExtractor {

    private static final Logger logger = LogManager.getLogger();

    private IdentifyingPropertyExtractor() { }

    /**
     * Extract a description of the identifying, volatile, and heuristic properties from an EntityDTO
     * given metadata describing the requested sets of properties.
     *
     * @param entityDTO the EntityDTO to extract the description from
     * @param identityMetadata metadata specifying the desired description of the entity
     * @return A {@code VMTEntityDescriptor} describing the input {@code EntityDTO}
     */
    public static EntityDescriptor extractEntityDescriptor(
            @Nonnull EntityDTO entityDTO,
            @Nonnull ServiceEntityIdentityMetadata identityMetadata) {
        List<PropertyDescriptor> identifyingProperties =
                extractProperties(identityMetadata.getNonVolatileProperties(), entityDTO);
        List<PropertyDescriptor> volatileProperties =
                extractProperties(identityMetadata.getVolatileProperties(), entityDTO);
        List<PropertyDescriptor> heuristicProperties =
                extractProperties(identityMetadata.getHeuristicProperties(), entityDTO);


        // Identifying properties are composed of [nonVolatile + volatile]
        identifyingProperties.addAll(volatileProperties);
        return new EntityDescriptorImpl(identifyingProperties, volatileProperties, heuristicProperties);
    }

    /**
     * Extract the properties from the DTO.
     *
     * @param properties The properties to extract
     * @param entityDTO The EntityDTO to extract properties from
     * @return The list of property descriptors for properties in entityDTO.
     */
    private static List<PropertyDescriptor> extractProperties(
            Collection<ServiceEntityProperty> properties,
            EntityDTO entityDTO) {
        List<PropertyDescriptor> retList = new ArrayList<>();
        for (ServiceEntityProperty property : properties) {
            List<Object> propertyValues = extract(entityDTO, property.name);
            if (propertyValues.isEmpty()) {
                logger.debug(() -> "No property named " + property.name + " to extract from DTO "
                                + entityDTO);
            }

            for (Object propertyValue : propertyValues) {
                String valueString = propertyValue == null ? null : propertyValue.toString();
                retList.add(new PropertyDescriptorImpl(valueString, property.groupId));
            }
        }
        return retList;
    }

    private static List<Object> extract(EntityDTO dto, String propertyName) {
        try {
            Property p = new Property(propertyName);
            Object value = p.getValue(dto);

            if (value instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> retList = (List<Object>)value;
                return retList;
            } else {
                return Collections.singletonList(value);
            }
        } catch (VMTExtractorException extractorException) {
            logger.error(
                    "Failed to extract property " + propertyName + " from DTO " +
                            dto + " with error " + extractorException
            );
        }

        return Collections.emptyList();
    }
}

package com.vmturbo.plan.orchestrator.templates;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecByEntityTypeRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateSpecsRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateSpec;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc.TemplateSpecServiceImplBase;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Implement gRpc service for template spec, provided different apis for getting template specs.
 */
public class TemplateSpecRpcService extends TemplateSpecServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final TemplateSpecParser templateSpecParser;

    public TemplateSpecRpcService(@Nonnull final TemplateSpecParser templateSpecParser) {
        this.templateSpecParser = templateSpecParser;
    }

    @Override
    public void getTemplateSpecs(GetTemplateSpecsRequest request,
                                 StreamObserver<TemplateSpec> responseObserver) {
        final Map<String, TemplateSpec> templateSpecMap = templateSpecParser.getTemplateSpecMap();
        templateSpecMap.entrySet().stream().forEach(entry -> responseObserver.onNext(entry.getValue()));
        responseObserver.onCompleted();
    }

    @Override
    public void getTemplateSpecByEntityType(GetTemplateSpecByEntityTypeRequest request,
                                            StreamObserver<TemplateSpec> responseObserver) {
        if(!request.hasEntityType()) {
            logger.error("Missing entity type for get template spec.");
            responseObserver.onError(Status.INVALID_ARGUMENT
            .withDescription("Get template spec need to provide entity type")
            .asException());
        }
        final Map<String, TemplateSpec> templateSpecMap = templateSpecParser.getTemplateSpecMap();

        String entityTypeName = EntityType.forNumber(request.getEntityType()).toString();
        Optional<TemplateSpec> templateSpec = Optional.ofNullable(templateSpecMap.get(entityTypeName));

        if(templateSpec.isPresent()) {
            responseObserver.onNext(templateSpec.get());
            responseObserver.onCompleted();
        }
        else {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("Template spec for entity type " + entityTypeName + " not found.")
                .asException());
        }
    }

    @Override
    public void getTemplateSpec(GetTemplateSpecRequest request,
                                StreamObserver<TemplateSpec> responseObserver) {
        if(!request.hasId()) {
            logger.error("Missing id for get template spec.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Get template spec need to provide id")
                .asException());
        }
        final Map<String, TemplateSpec> templateSpecMap = templateSpecParser.getTemplateSpecMap();
        Optional<TemplateSpec> templateSpec = templateSpecMap.entrySet().stream()
            .map(Entry::getValue)
            .filter(templateSpecObj -> templateSpecObj.getId() == request.getId())
            .findFirst();
        if(templateSpec.isPresent()) {
            responseObserver.onNext(templateSpec.get());
            responseObserver.onCompleted();
        }
        else {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("Template spec for id " + request.getId() + " not found.")
                .asException());
        }
    }
}

package software.amazon.kendraranking.executionplan;

import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public interface ExecutionPlanArnBuilder {
  String build(ResourceHandlerRequest<ResourceModel> request);
}


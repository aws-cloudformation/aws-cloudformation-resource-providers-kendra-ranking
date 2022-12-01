package software.amazon.kendraranking.executionplan;

import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class TestExecutionArnBuilder implements ExecutionPlanArnBuilder {

  @Override
  public String build(ResourceHandlerRequest<ResourceModel> request) {
    return String.format("arn:aws:kendra-ranking:us-west-2:0123456789:rescore-execution-plan/%s", request.getDesiredResourceState().getId());
  }
}


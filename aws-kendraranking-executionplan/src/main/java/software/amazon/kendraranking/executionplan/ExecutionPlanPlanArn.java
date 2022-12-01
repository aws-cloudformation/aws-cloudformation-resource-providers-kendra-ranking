package software.amazon.kendraranking.executionplan;

import lombok.NonNull;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ExecutionPlanPlanArn implements ExecutionPlanArnBuilder {
  private String planArnFormat = "arn:%s:kendra-ranking:%s:%s:rescore-execution-plan/%s";

  @Override
  public String build(ResourceHandlerRequest<ResourceModel> request) {
    return build(request.getAwsPartition(), request.getRegion(),
        request.getAwsAccountId(), request.getDesiredResourceState().getId());
  }

  private String build(@NonNull String partition, @NonNull String region,
      @NonNull String accountId, @NonNull String executionPlanId) {
    return String.format(planArnFormat, partition, region, accountId, executionPlanId);
  }
}


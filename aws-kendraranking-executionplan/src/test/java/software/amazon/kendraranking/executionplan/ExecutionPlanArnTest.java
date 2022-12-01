package software.amazon.kendraranking.executionplan;

import org.junit.jupiter.api.Test;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExecutionPlanArnTest {

  @Test
  public void testBuild() {
    String partition = "aws";
    String region = "us-west-2";
    String accountId = "0123456789";
    String executionPlanId = "0123456789abcdef";
    ResourceHandlerRequest<ResourceModel> request = new ResourceHandlerRequest<>();
    request.setAwsPartition(partition);
    request.setAwsAccountId(accountId);
    request.setRegion(region);
    request.setDesiredResourceState(ResourceModel.builder().id(executionPlanId).build());
    assertThat(new ExecutionPlanPlanArn().build(request))
        .isEqualTo("arn:aws:kendra-ranking:us-west-2:0123456789:rescore-execution-plan/0123456789abcdef");
  }


  @Test
  public void testBuildThrowsExceptionWhenPartitionIsNull() {
    String region = "us-west-2";
    String accountId = "0123456789";
    String executionPlanId = "0123456789abcdef";
    ResourceHandlerRequest<ResourceModel> request = new ResourceHandlerRequest<>();
    request.setAwsPartition(null);
    request.setAwsAccountId(accountId);
    request.setRegion(region);
    request.setDesiredResourceState(ResourceModel.builder().id(executionPlanId).build());
    assertThrows(NullPointerException.class, () -> {
      new ExecutionPlanPlanArn().build(request);
    });
  }

  @Test
  public void testBuildThrowsExceptionWhenRegionIsNull() {
    String partition = "aws";
    String accountId = "0123456789";
    String executionPlanId = "0123456789abcdef";
    ResourceHandlerRequest<ResourceModel> request = new ResourceHandlerRequest<>();
    request.setAwsPartition(partition);
    request.setAwsAccountId(accountId);
    request.setRegion(null);
    request.setDesiredResourceState(ResourceModel.builder().id(executionPlanId).build());
    assertThrows(NullPointerException.class, () -> {
      new ExecutionPlanPlanArn().build(request);
    });
  }

  @Test
  public void testBuildThrowsExceptionWhenAccountIdIsNull() {
    String partition = "aws";
    String region = "us-west-2";
    String executionPlanId = "0123456789abcdef";
    ResourceHandlerRequest<ResourceModel> request = new ResourceHandlerRequest<>();
    request.setAwsPartition(partition);
    request.setAwsAccountId(null);
    request.setRegion(region);
    request.setDesiredResourceState(ResourceModel.builder().id(executionPlanId).build());
    assertThrows(NullPointerException.class, () -> {
      new ExecutionPlanPlanArn().build(request);
    });
  }

  @Test
  public void testBuildThrowsExceptionWhenPlanIdIsNull() {
    String partition = "aws";
    String region = "us-west-2";
    String accountId = "0123456789";
    ResourceHandlerRequest<ResourceModel> request = new ResourceHandlerRequest<>();
    request.setAwsPartition(partition);
    request.setAwsAccountId(accountId);
    request.setRegion(region);
    request.setDesiredResourceState(ResourceModel.builder().id(null).build());
    assertThrows(NullPointerException.class, () -> {
      new ExecutionPlanPlanArn().build(request);
    });
  }

}


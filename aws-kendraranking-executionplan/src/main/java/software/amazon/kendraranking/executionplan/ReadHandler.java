package software.amazon.kendraranking.executionplan;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraranking.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kendraranking.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.kendraranking.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kendraranking.KendraRankingClient;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanRequest;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static software.amazon.kendraranking.executionplan.ApiName.DESCRIBE_EXECUTION_PLAN;
import static software.amazon.kendraranking.executionplan.ApiName.LIST_TAGS_FOR_RESOURCE;

public class ReadHandler extends BaseHandlerStd {

  private ExecutionPlanArnBuilder executionPlanArnBuilder;

  public ReadHandler() {
    this(new ExecutionPlanPlanArn());
  }

  public ReadHandler(ExecutionPlanArnBuilder executionPlanArnBuilder) {
    super();
    this.executionPlanArnBuilder = executionPlanArnBuilder;
  }

  protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
      final AmazonWebServicesClientProxy proxy,
      final ResourceHandlerRequest<ResourceModel> request,
      final CallbackContext callbackContext,
      final ProxyClient<KendraRankingClient> proxyClient,
      final Logger logger) {

    final ResourceModel model = request.getDesiredResourceState();
    final DescribeRescoreExecutionPlanRequest describeRescoreExecutionPlanRequest = Translator.translateToReadRequest(model);
    DescribeRescoreExecutionPlanResponse describeRescoreExecutionPlanResponse;
    try {
      describeRescoreExecutionPlanResponse = proxyClient.injectCredentialsAndInvokeV2(
          describeRescoreExecutionPlanRequest, proxyClient.client()::describeRescoreExecutionPlan);
    } catch (ResourceNotFoundException e) {
      throw new CfnNotFoundException(ResourceModel.TYPE_NAME, describeRescoreExecutionPlanRequest.id(), e);
    } catch (final AwsServiceException e) { // ResourceNotFoundException
      /*
       * While the handler contract states that the handler must always return a progress event,
       * you may throw any instance of BaseHandlerException, as the wrapper map it to a progress event.
       * Each BaseHandlerException maps to a specific error code, and you should map service exceptions as closely as possible
       * to more specific error codes
       */
      throw new CfnGeneralServiceException(DESCRIBE_EXECUTION_PLAN, e); // e.g. https://github.com/aws-cloudformation/aws-cloudformation-resource-providers-logs/commit/2077c92299aeb9a68ae8f4418b5e932b12a8b186#diff-5761e3a9f732dc1ef84103dc4bc93399R56-R63
    }

    String executionPlanArn = executionPlanArnBuilder.build(request);
    final ListTagsForResourceRequest listTagsForResourceRequest = Translator.translateToListTagsRequest(executionPlanArn);
    ListTagsForResourceResponse listTagsForResourceResponse;
    try {
      listTagsForResourceResponse = proxyClient.injectCredentialsAndInvokeV2(listTagsForResourceRequest,
          proxyClient.client()::listTagsForResource);
    } catch (AwsServiceException e) {
      throw new CfnGeneralServiceException(LIST_TAGS_FOR_RESOURCE, e);
    }

    return constructResourceModelFromResponse(describeRescoreExecutionPlanResponse, listTagsForResourceResponse, executionPlanArn);
  }

  /**
   * Implement client invocation of the read request through the proxyClient, which is already initialised with
   * caller credentials, correct region and retry settings
   * @param describeRescoreExecutionPlanResponse the aws service describe resource response
   * @return progressEvent indicating success, in progress with delay callback or failed state
   */
  private ProgressEvent<ResourceModel, CallbackContext> constructResourceModelFromResponse(
      final DescribeRescoreExecutionPlanResponse describeRescoreExecutionPlanResponse,
      final ListTagsForResourceResponse listTagsForResourceResponse,
      String executionPlanArn) {
    ResourceModel resourceModel = Translator.translateFromReadResponse(describeRescoreExecutionPlanResponse,
        listTagsForResourceResponse,
        executionPlanArn);
    return ProgressEvent.defaultSuccessHandler(resourceModel);
  }
}

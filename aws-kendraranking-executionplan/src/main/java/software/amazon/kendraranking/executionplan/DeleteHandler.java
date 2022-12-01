package software.amazon.kendraranking.executionplan;

import static software.amazon.kendraranking.executionplan.ApiName.DELETE_EXECUTION_PLAN;

import java.time.Duration;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kendraintelligentranking.KendraIntelligentRankingClient;
import software.amazon.awssdk.services.kendraintelligentranking.model.ConflictException;
import software.amazon.awssdk.services.kendraintelligentranking.model.DeleteRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraintelligentranking.model.DeleteRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraintelligentranking.model.DescribeRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraintelligentranking.model.ResourceNotFoundException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Delay;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
public class DeleteHandler extends BaseHandlerStd {
  private static final Constant STABILIZATION_DELAY = Constant.of()
      // Set the timeout to something silly/way too high, because
      // we already set the timeout in the schema https://github.com/aws-cloudformation/aws-cloudformation-resource-schema
      .timeout(Duration.ofDays(365L))
      // Set the delay to two minutes so the stabilization code only calls
      // DescribeRescoreExecutionPlan every one minute
      .delay(Duration.ofMinutes(1))
      .build();

    private Delay delay;

    public DeleteHandler() {
      this(STABILIZATION_DELAY);
    }

    public DeleteHandler(Delay delay) {
      super();
      this.delay = delay;
    }

  protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
      final AmazonWebServicesClientProxy proxy,
      final ResourceHandlerRequest<ResourceModel> request,
      final CallbackContext callbackContext,
      final ProxyClient<KendraIntelligentRankingClient> proxyClient,
      final Logger logger) {

    final ResourceModel model = request.getDesiredResourceState();

    // TODO: Adjust Progress Chain according to your implementation
    // https://github.com/aws-cloudformation/cloudformation-cli-java-plugin/blob/master/src/main/java/software/amazon/cloudformation/proxy/CallChain.java

    return ProgressEvent.progress(model, callbackContext)

        // STEP 1 [check if resource already exists]
        // for more information -> https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-test-contract.html
        // if target API does not support 'ResourceNotFoundException' then following check is required
        //.then(progress -> checkForPreDeleteResourceExistence(proxy, proxyClient, request, progress))
        .then(progress -> preExistenceCheckForDelete(proxy, proxyClient, progress, request, logger))
        // STEP 2.0 [delete/stabilize progress chain - required for resource deletion]
        .then(progress ->
            // If your service API throws 'ResourceNotFoundException' for delete requests then DeleteHandler can return just proxy.initiate construction
            // STEP 2.0 [initialize a proxy context]
            proxy.initiate("AWS-KendraRanking-ExecutionPlan::Delete", proxyClient, model, callbackContext)
                // STEP 2.1 [TODO: construct a body of a request]
                .translateToServiceRequest(Translator::translateToDeleteRequest)
                .backoffDelay(delay)
                // STEP 2.2 [TODO: make an api call]
                .makeServiceCall((awsRequest, sdkProxyClient) -> deleteExecutionPlan(awsRequest, sdkProxyClient, callbackContext, logger))
                // STEP 2.3 [TODO: stabilize step is not necessarily required but typically involves describing the resource until it is in a certain status, though it can take many forms]
                // for more information -> https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-test-contract.html
                .stabilize((deleteRescoreExecutionPlanRequest,
                    deleteRescoreExecutionPlanResponse,
                    kendrarankingProxyClient,
                    modelStabilize,
                    callbackContextStablize) -> stabilizedOnDelete(deleteRescoreExecutionPlanRequest,
                    deleteRescoreExecutionPlanResponse,
                    kendrarankingProxyClient,
                    modelStabilize,
                    callbackContextStablize,
                    logger))
                .done(this::setResourceModelToNullAndReturnSuccess));
  }

  private ProgressEvent<ResourceModel, CallbackContext> preExistenceCheckForDelete(
      final AmazonWebServicesClientProxy proxy,
      final ProxyClient<KendraIntelligentRankingClient> proxyClient,
      final ProgressEvent<ResourceModel, CallbackContext> progressEvent,
      final ResourceHandlerRequest<ResourceModel> request,
      Logger logger
  ) {
    ResourceModel model = progressEvent.getResourceModel();
    CallbackContext callbackContext = progressEvent.getCallbackContext();

    logger.log(String.format("%s [%s] pre-existence check for deletion", ResourceModel.TYPE_NAME, model.getPrimaryIdentifier()));

    DescribeRescoreExecutionPlanRequest describeRescoreExecutionPlanRequest = DescribeRescoreExecutionPlanRequest.builder()
        .id(model.getId())
        .build();
    try {
      proxyClient.injectCredentialsAndInvokeV2(describeRescoreExecutionPlanRequest,
          proxyClient.client()::describeRescoreExecutionPlan);
      return ProgressEvent.progress(model, callbackContext);
    } catch (ResourceNotFoundException e) {
      if (callbackContext.isDeleteWorkflow()) {
        logger.log(String.format("In a delete workflow. Allow ResourceNotFoundException to propagate."));
        return ProgressEvent.progress(model, callbackContext);
      }
      logger.log(String.format("%s [%s] does not pre-exist", ResourceModel.TYPE_NAME, model.getPrimaryIdentifier()));
      throw new CfnNotFoundException(ResourceModel.TYPE_NAME, describeRescoreExecutionPlanRequest.id(), e);
    }
  }

  private ProgressEvent<ResourceModel, CallbackContext> setResourceModelToNullAndReturnSuccess(
      DeleteRescoreExecutionPlanRequest deleteRescoreExecutionPlanRequest,
      DeleteRescoreExecutionPlanResponse deleteRescoreExecutionPlanResponse,
      ProxyClient<KendraIntelligentRankingClient> proxyClient,
      ResourceModel resourceModel,
      CallbackContext callbackContext) {
    return ProgressEvent.defaultSuccessHandler(null);
  }

  /**
   * Implement client invocation of the delete request through the proxyClient, which is already initialised with
   * caller credentials, correct region and retry settings
   * @param deleteRescoreExecutionPlanRequest the aws service request to delete a resource
   * @param proxyClient the aws service client to make the call
   * @return delete resource response
   */
  private DeleteRescoreExecutionPlanResponse deleteExecutionPlan(
      final DeleteRescoreExecutionPlanRequest deleteRescoreExecutionPlanRequest,
      final ProxyClient<KendraIntelligentRankingClient> proxyClient,
      final CallbackContext callbackContext,
      final Logger logger) {
    DeleteRescoreExecutionPlanResponse deleteRescoreExecutionPlanResponse;
    try {
      deleteRescoreExecutionPlanResponse = proxyClient.injectCredentialsAndInvokeV2(
          deleteRescoreExecutionPlanRequest, proxyClient.client()::deleteRescoreExecutionPlan);
      callbackContext.setDeleteWorkflow(true);
    } catch (ResourceNotFoundException e) {
      // If the plan didn't exist before the delete request
      throw new CfnNotFoundException(ResourceModel.TYPE_NAME, deleteRescoreExecutionPlanRequest.id(), e);
    } catch (ConflictException e) {
      // Execution plan must be in ACTIVE state before initiating delete
      throw new CfnResourceConflictException(e);
    } catch (final AwsServiceException e) {
      /*
       * While the handler contract states that the handler must always return a progress event,
       * you may throw any instance of BaseHandlerException, as the wrapper map it to a progress event.
       * Each BaseHandlerException maps to a specific error code, and you should map service exceptions as closely as possible
       * to more specific error codes
       */
      throw new CfnGeneralServiceException(DELETE_EXECUTION_PLAN, e);
    }

    logger.log(String.format("%s successfully called DeleterescoreExecutionPlan with ID %s. Still need to stabilize.",
        ResourceModel.TYPE_NAME, deleteRescoreExecutionPlanRequest.id()));
    return deleteRescoreExecutionPlanResponse;
  }

  /**
   * If deletion of your resource requires some form of stabilization (e.g. propagation delay)
   * for more information -> https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-test-contract.html
   * @param deleteRescoreExecutionPlanRequest the aws service request to delete a resource
   * @param deleteRescoreExecutionPlanResponse the aws service response to delete a resource
   * @param proxyClient the aws service client to make the call
   * @param model resource model
   * @param callbackContext callback context
   * @return boolean state of stabilized or not
   */
  private boolean stabilizedOnDelete(
      final DeleteRescoreExecutionPlanRequest deleteRescoreExecutionPlanRequest,
      final DeleteRescoreExecutionPlanResponse deleteRescoreExecutionPlanResponse,
      final ProxyClient<KendraIntelligentRankingClient> proxyClient,
      final ResourceModel model,
      final CallbackContext callbackContext,
      final Logger logger) {

    DescribeRescoreExecutionPlanRequest describeRescoreExecutionPlanRequest = DescribeRescoreExecutionPlanRequest.builder()
        .id(model.getId())
        .build();
    boolean stabilized;
    try {
      proxyClient.injectCredentialsAndInvokeV2(describeRescoreExecutionPlanRequest,
          proxyClient.client()::describeRescoreExecutionPlan);
      stabilized = false;
    } catch (ResourceNotFoundException e) {
      stabilized = true;
    }
    logger.log(String.format("%s [%s] deletion has stabilized: %s", ResourceModel.TYPE_NAME, model.getPrimaryIdentifier(), stabilized));
    return stabilized;
  }
}

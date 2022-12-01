package software.amazon.kendraranking.executionplan;

import static software.amazon.kendraranking.executionplan.ApiName.UPDATE_EXECUTION_PLAN;

import com.google.common.collect.Sets;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kendraranking.KendraRankingClient;
import software.amazon.awssdk.services.kendraranking.model.AccessDeniedException;
import software.amazon.awssdk.services.kendraranking.model.ConflictException;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraranking.model.RescoreExecutionPlanStatus;
import software.amazon.awssdk.services.kendraranking.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kendraranking.model.ServiceQuotaExceededException;
import software.amazon.awssdk.services.kendraranking.model.TagResourceRequest;
import software.amazon.awssdk.services.kendraranking.model.UntagResourceRequest;
import software.amazon.awssdk.services.kendraranking.model.UpdateRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.UpdateRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraranking.model.ValidationException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Delay;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
public class UpdateHandler extends BaseHandlerStd {
  private static final Constant STABILIZATION_DELAY = Constant.of()
      // Set the timeout to something silly/way too high, because
      // we already set the timeout in the schema https://github.com/aws-cloudformation/aws-cloudformation-resource-schema
      .timeout(Duration.ofDays(365L))
      // Set the delay to two minutes so the stabilization code only calls
      // DescribeRescoreExecutionPlan every one minute
      .delay(Duration.ofMinutes(1))
      .build();

  private Delay delay;

  private ExecutionPlanArnBuilder executionPlanArnBuilder;

  public UpdateHandler() {
    this(new ExecutionPlanPlanArn(), STABILIZATION_DELAY);
  }

  public UpdateHandler(ExecutionPlanArnBuilder executionPlanArnBuilder, Delay delay) {
    super();
    this.executionPlanArnBuilder = executionPlanArnBuilder;
    this.delay = delay;
  }


  protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<KendraRankingClient> proxyClient,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        return ProgressEvent.progress(model, callbackContext)
            // First validate the resource actually exists per the contract requirements
            // https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-test-contract.html
            .then(progress ->
                proxy.initiate("AWS-KendraRanking-ExecutionPlan::ValidateResourceExists", proxyClient, model, callbackContext)
                    .translateToServiceRequest(resourceModel -> Translator.translateToReadRequest(model))
                    .makeServiceCall(this::validateResourceExists)
                    .progress())
            .then(progress ->
                proxy.initiate("AWS-KendraRanking-ExecutionPlan::Update", proxyClient, model, callbackContext)
                    .translateToServiceRequest(resourceModel -> translateToUpdateRequest(model, request.getPreviousResourceState()))
                    .backoffDelay(delay)
                    .makeServiceCall((updateRescoreExecutionPlanRequest, kendraRankingClientProxyClient)
                        -> updateExecutionPlan(updateRescoreExecutionPlanRequest, kendraRankingClientProxyClient, logger))
                    .stabilize((updateRescoreExecutionPlanRequest,
                        updateRescoreExecutionPlanResponse,
                        rankingClientProxyClient,
                        resourceModel,
                        context) -> stabilize(updateRescoreExecutionPlanRequest,
                        updateRescoreExecutionPlanResponse,
                        rankingClientProxyClient,
                        resourceModel,
                        context,
                        logger))
                    .progress())
            .then(progress -> updateTags(proxyClient, progress, request))
            .then(progress -> new ReadHandler(executionPlanArnBuilder).handleRequest(proxy, request, callbackContext, proxyClient, logger));

    }

    private DescribeRescoreExecutionPlanResponse validateResourceExists(DescribeRescoreExecutionPlanRequest describeRescoreExecutionPlanRequest,
        ProxyClient<KendraRankingClient> proxyClient) {
      DescribeRescoreExecutionPlanResponse describeRescoreExecutionPlanResponse;
      try {
        describeRescoreExecutionPlanResponse = proxyClient.injectCredentialsAndInvokeV2(
            describeRescoreExecutionPlanRequest,proxyClient.client()::describeRescoreExecutionPlan);
      } catch (ResourceNotFoundException e) {
        throw new CfnNotFoundException(ResourceModel.TYPE_NAME, describeRescoreExecutionPlanRequest.id(), e);
      }
      return describeRescoreExecutionPlanResponse;
    }

    static UpdateRescoreExecutionPlanRequest translateToUpdateRequest(final ResourceModel model,
        final ResourceModel prevModel) {
      try {
        return Translator.translateToUpdateRequest(model, prevModel);
      } catch (TranslatorValidationException e) {
        throw new CfnInvalidRequestException(e.getMessage(), e);
      }
    }

    private boolean stabilize(
        final UpdateRescoreExecutionPlanRequest updateRescoreExecutionPlanRequest,
        final UpdateRescoreExecutionPlanResponse updateRescoreExecutionPlanResponse,
        final ProxyClient<KendraRankingClient> proxyClient,
        final ResourceModel model,
        final CallbackContext callbackContext,
        final Logger logger) {
      DescribeRescoreExecutionPlanRequest describeRescoreExecutionPlanRequest = DescribeRescoreExecutionPlanRequest.builder()
          .id(model.getId())
          .build();
      DescribeRescoreExecutionPlanResponse describeRescoreExecutionPlanResponse = proxyClient.injectCredentialsAndInvokeV2(
          describeRescoreExecutionPlanRequest, proxyClient.client()::describeRescoreExecutionPlan);
      RescoreExecutionPlanStatus rescoreExecutionPlanStatus = describeRescoreExecutionPlanResponse.status();
      boolean stabilized = rescoreExecutionPlanStatus.equals(RescoreExecutionPlanStatus.ACTIVE);
      logger.log(String.format("%s [%s] update has stabilized: %s", ResourceModel.TYPE_NAME, model.getPrimaryIdentifier(), stabilized));
      return stabilized;
    }

    /**
     * Implement client invocation of the update request through the proxyClient, which is already initialised with
     * caller credentials, correct region and retry settings
     * @param updateRescoreExecutionPlanRequest the aws service request to update a resource
     * @param proxyClient the aws service client to make the call
     * @return update resource response
     */
    private UpdateRescoreExecutionPlanResponse updateExecutionPlan(
        final UpdateRescoreExecutionPlanRequest updateRescoreExecutionPlanRequest,
        final ProxyClient<KendraRankingClient> proxyClient,
        final Logger logger) {
      UpdateRescoreExecutionPlanResponse updateRescoreExecutionPlanResponse;
      // In this code block we assume the previous DescribeExecutionPlan API call validated the resource exists and so doesn't
      // catch and re-throw here.
      try {
        updateRescoreExecutionPlanResponse = proxyClient.injectCredentialsAndInvokeV2(updateRescoreExecutionPlanRequest,
            proxyClient.client()::updateRescoreExecutionPlan);
      } catch (ValidationException e) {
        throw new CfnInvalidRequestException(e.getMessage(), e);
      } catch (AccessDeniedException e) {
        throw new CfnAccessDeniedException(e.getMessage(), e);
      } catch (ConflictException e) {
        throw new CfnResourceConflictException(e);
      } catch (ServiceQuotaExceededException e) {
        throw new CfnServiceLimitExceededException(ResourceModel.TYPE_NAME, e.getMessage(), e.getCause());
      } catch (final AwsServiceException e) {
        /*
         * While the handler contract states that the handler must always return a progress event,
         * you may throw any instance of BaseHandlerException, as the wrapper map it to a progress event.
         * Each BaseHandlerException maps to a specific error code, and you should map service exceptions as closely as possible
         * to more specific error codes
         */
        throw new CfnGeneralServiceException(UPDATE_EXECUTION_PLAN, e);
      }

      logger.log(String.format("%s has successfully been updated.", ResourceModel.TYPE_NAME));
      return updateRescoreExecutionPlanResponse;
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateTags(
        final ProxyClient<KendraRankingClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        ResourceHandlerRequest<ResourceModel> request) {
      CallbackContext callbackContext = progress.getCallbackContext();
      ResourceModel currResourceModel = request.getDesiredResourceState();
      ResourceModel prevResourceModel = request.getPreviousResourceState();
      Set<Tag> currentTags;
      if (currResourceModel.getTags() != null) {
        currentTags = currResourceModel.getTags().stream().collect(Collectors.toSet());
      } else {
        currentTags = new HashSet<>();
      }

      String arn = executionPlanArnBuilder.build(request);
      Set<Tag> existingTags = new HashSet<>();
      if (prevResourceModel != null && prevResourceModel.getTags() != null) {
        existingTags = prevResourceModel.getTags().stream().collect(Collectors.toSet());
      }
      final Set<Tag> tagsToAdd = Sets.difference(currentTags, existingTags);
      if (!tagsToAdd.isEmpty()) {
        TagResourceRequest tagResourceRequest = Translator.translateToTagResourceRequest(tagsToAdd, arn);
        try {
          proxyClient.injectCredentialsAndInvokeV2(tagResourceRequest, proxyClient.client()::tagResource);
        } catch (ValidationException e) {
          throw new CfnInvalidRequestException(e.getMessage(), e);
        }
      }

      final Set<Tag> tagsToRemove = Sets.difference(existingTags, currentTags);
      if (!tagsToRemove.isEmpty()) {
        UntagResourceRequest untagResourceRequest = Translator.translateToUntagResourceRequest(tagsToRemove, arn);
        try {
          proxyClient.injectCredentialsAndInvokeV2(untagResourceRequest, proxyClient.client()::untagResource);
        } catch (ValidationException e) {
          throw new CfnInvalidRequestException(e.getMessage(), e);
        }
      }
      return ProgressEvent.progress(currResourceModel, callbackContext);
    }
}

package software.amazon.kendraranking.executionplan;

import static software.amazon.kendraranking.executionplan.ApiName.UPDATE_EXECUTION_PLAN;

import com.google.common.collect.Sets;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.kendraranking.KendraRankingClient;
import software.amazon.awssdk.services.kendraranking.model.AccessDeniedException;
import software.amazon.awssdk.services.kendraranking.model.ConflictException;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraranking.model.RescoreExecutionPlanStatus;
import software.amazon.awssdk.services.kendraranking.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kendraranking.model.ServiceQuotaExceededException;
import software.amazon.awssdk.services.kendraranking.model.TagResourceRequest;
import software.amazon.awssdk.services.kendraranking.model.ThrottlingException;
import software.amazon.awssdk.services.kendraranking.model.UntagResourceRequest;
import software.amazon.awssdk.services.kendraranking.model.UpdateRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.UpdateRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraranking.model.ValidationException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.exceptions.CfnThrottlingException;
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
      .delay(Duration.ofMinutes(2))
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

  private static final BiFunction<ResourceModel, ProxyClient<KendraRankingClient>, ResourceModel> EMPTY_CALL =
      (model, proxyClient) -> model;


  protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<KendraRankingClient> proxyClient,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
//        logger.log("Update has been called>>>>>>");
//        logger.log("DESIRED STATE>>>>" + model);

        return ProgressEvent.progress(model, callbackContext)
            // First validate the resource actually exists per the contract requirements
            // https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-test-contract.html
            .then(progress ->
                proxy.initiate("AWS-KendraRanking-ExecutionPlan::ValidateResourceExists", proxyClient, model, callbackContext)
                    .translateToServiceRequest(resourceModel -> Translator.translateToReadRequest(model))
                    .makeServiceCall((describeRescoreExecutionPlanRequest, kendraRankingClientProxyClient) ->
                        validateResourceExists(describeRescoreExecutionPlanRequest,kendraRankingClientProxyClient,logger) )
                    .progress())
            .then(progress ->
                proxy.initiate("AWS-KendraRanking-ExecutionPlan::Update", proxyClient, model, callbackContext)
                    .translateToServiceRequest(resourceModel -> translateToUpdateRequest(model, request.getPreviousResourceState()))
                    .backoffDelay(delay)
                    .makeServiceCall((updateRescoreExecutionPlanRequest, kendraRankingClientProxyClient)
                        -> updateExecutionPlan(updateRescoreExecutionPlanRequest, kendraRankingClientProxyClient, logger))
                    .progress())

            .then(progress -> stabilize(proxy, proxyClient, progress, "AWS-KendraRanking-ExecutionPlan::PostUpdateStabilize", logger))
            .then(progress -> updateTags(proxyClient, progress, request, logger))
            .then(progress -> new ReadHandler(executionPlanArnBuilder).handleRequest(proxy, request, callbackContext, proxyClient, logger));

    }

    private DescribeRescoreExecutionPlanResponse validateResourceExists(DescribeRescoreExecutionPlanRequest describeRescoreExecutionPlanRequest,
        ProxyClient<KendraRankingClient> proxyClient, final Logger logger) {
    //logger.log("In validateResource");
      DescribeRescoreExecutionPlanResponse describeRescoreExecutionPlanResponse = null;
      try {
        describeRescoreExecutionPlanResponse = proxyClient.injectCredentialsAndInvokeV2(
            describeRescoreExecutionPlanRequest,proxyClient.client()::describeRescoreExecutionPlan);
      } catch (ResourceNotFoundException e) {
        throw new CfnNotFoundException(ResourceModel.TYPE_NAME, describeRescoreExecutionPlanRequest.id(), e);
      } catch (ThrottlingException e) {
        throw RetryableException.builder().cause(e).build();
      }
      catch  (AwsServiceException e) {
        //logger.log("ERROR while describe>>>>>" + e);
        throw new CfnGeneralServiceException(UPDATE_EXECUTION_PLAN, e);
      }

      //logger.log("describeRescoreExecutionPlanResponse>>>>" + describeRescoreExecutionPlanResponse);
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

  private ProgressEvent<ResourceModel, CallbackContext> stabilize(
      final AmazonWebServicesClientProxy proxy,
      final ProxyClient<KendraRankingClient> proxyClient,
      final ProgressEvent<ResourceModel, CallbackContext> progress,
      String callGraph,
      final Logger logger) {
//    logger.log("In Stabilize>>>>>");
//    logger.log("PROGRESS>>>>" + progress.toString());
    return proxy.initiate(callGraph, proxyClient, progress.getResourceModel(),
            progress.getCallbackContext())
        .translateToServiceRequest(Function.identity())
        .backoffDelay(delay)
        .makeServiceCall(EMPTY_CALL)
        .stabilize((request, response, proxyInvocation, model, callbackContext) ->
            isStabilized(proxyInvocation, model, logger)).progress();
  }

  private boolean isStabilized(final ProxyClient<KendraRankingClient> proxyClient,
      final ResourceModel model,
      final Logger logger) {
    //logger.log("In isStablilized");
    DescribeRescoreExecutionPlanRequest describeRescoreExecutionPlanRequest = DescribeRescoreExecutionPlanRequest.builder()
        .id(model.getId())
        .build();

    //logger.log("In stabilized and calling describe>>>>" + describeRescoreExecutionPlanRequest);
//    final long startTime = System.currentTimeMillis();
//    logger.log("StartTIME>>>>" + startTime);

    DescribeRescoreExecutionPlanResponse describeRescoreExecutionPlanResponse = null;
    try {
      describeRescoreExecutionPlanResponse = proxyClient.injectCredentialsAndInvokeV2(describeRescoreExecutionPlanRequest,
          proxyClient.client()::describeRescoreExecutionPlan);
    } catch (ThrottlingException e) {
      return false;
    }
//    logger.log("describeRescoreExecutionPlanResponse>>>>>>" + describeRescoreExecutionPlanResponse);
//    final long endTime = System.currentTimeMillis();
//    logger.log("ENDTIME>>>>" + endTime);
    RescoreExecutionPlanStatus rerankingEndpointStatus = describeRescoreExecutionPlanResponse.status();
    if (RescoreExecutionPlanStatus.FAILED.equals(rerankingEndpointStatus)) {
      throw new CfnNotStabilizedException(ResourceModel.TYPE_NAME, model.getId());
    }

    //logger.log("STATUS>>>" +  rerankingEndpointStatus);
    boolean stabilized = RescoreExecutionPlanStatus.ACTIVE.equals(rerankingEndpointStatus);
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
//      logger.log("calling update>>>>");
//      logger.log("updateRescoreExecutionPlanRequest>>>>>" + updateRescoreExecutionPlanRequest.toString());
      UpdateRescoreExecutionPlanResponse updateRescoreExecutionPlanResponse = null;
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
      } catch (ThrottlingException e) {
        throw RetryableException.builder().cause(e).build();
      }
      catch (final AwsServiceException e) {
        /*
         * While the handler contract states that the handler must always return a progress event,
         * you may throw any instance of BaseHandlerException, as the wrapper map it to a progress event.
         * Each BaseHandlerException maps to a specific error code, and you should map service exceptions as closely as possible
         * to more specific error codes
         */
        //logger.log("AwsServiceException>>>>>"+ e);
        throw new CfnGeneralServiceException(UPDATE_EXECUTION_PLAN, e);
      }
//      catch (Exception e) {
//        //logger.log("EXCEPTION IN UPDATE>>>>" + e);
//      }
      //logger.log("updateRescoreExecutionPlanResponse>>>>" + updateRescoreExecutionPlanResponse);
//      if (updateRescoreExecutionPlanResponse != null) {
//        logger.log("updateRescoreExecutionPlanResponse metadata>>>>" + updateRescoreExecutionPlanResponse.responseMetadata());
//      }
      logger.log(String.format("%s update has been called successfully.", ResourceModel.TYPE_NAME));
      return updateRescoreExecutionPlanResponse;
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateTags(
        final ProxyClient<KendraRankingClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        ResourceHandlerRequest<ResourceModel> request, Logger logger) {
      CallbackContext callbackContext = progress.getCallbackContext();
      ResourceModel currResourceModel = request.getDesiredResourceState();
      ResourceModel prevResourceModel = request.getPreviousResourceState();
      Set<Tag> currentTags;
      //logger.log("In update tags");
      if (currResourceModel.getTags() != null) {
        //logger.log("current tags>>>>>" + currResourceModel.getTags().toString());

        currentTags = currResourceModel.getTags().stream().collect(Collectors.toSet());
      } else {
        currentTags = new HashSet<>();
      }


      String arn = executionPlanArnBuilder.build(request);
      Set<Tag> existingTags = new HashSet<>();
      if (prevResourceModel != null && prevResourceModel.getTags() != null) {
        //logger.log("previous tags>>>>>" + prevResourceModel.getTags().toString());

        existingTags = prevResourceModel.getTags().stream().collect(Collectors.toSet());
      }
      final Set<Tag> tagsToAdd = Sets.difference(currentTags, existingTags);
      //logger.log("tagsToAdd>>>>" + tagsToAdd.toString());
      if (!tagsToAdd.isEmpty()) {
        TagResourceRequest tagResourceRequest = Translator.translateToTagResourceRequest(tagsToAdd, arn);
        try {
          proxyClient.injectCredentialsAndInvokeV2(tagResourceRequest, proxyClient.client()::tagResource);
        } catch (ValidationException e) {
          throw new CfnInvalidRequestException(e.getMessage(), e);
        }
      }

      final Set<Tag> tagsToRemove = Sets.difference(existingTags, currentTags);
      //logger.log("tagstoremove>>>>" + tagsToRemove.toString());
      if (!tagsToRemove.isEmpty()) {
        UntagResourceRequest untagResourceRequest = Translator.translateToUntagResourceRequest(tagsToRemove, arn);
        try {
          proxyClient.injectCredentialsAndInvokeV2(untagResourceRequest, proxyClient.client()::untagResource);
        } catch (ValidationException e) {
          throw new CfnInvalidRequestException(e.getMessage(), e);
        }
      }
      //logger.log("currResourceModel>>>>" + currResourceModel.getTags().toString());
      return ProgressEvent.progress(currResourceModel, callbackContext);
    }
}

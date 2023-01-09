package software.amazon.kendraranking.executionplan;

import static software.amazon.kendraranking.executionplan.ApiName.CREATE_EXECUTION_PLAN;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.kendraranking.model.AccessDeniedException;
import software.amazon.awssdk.services.kendraranking.model.ConflictException;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraranking.model.RescoreExecutionPlanStatus;
import software.amazon.awssdk.services.kendraranking.model.ServiceQuotaExceededException;
import software.amazon.awssdk.services.kendraranking.model.ThrottlingException;
import software.amazon.awssdk.services.kendraranking.model.ValidationException;
import software.amazon.awssdk.services.kendraranking.KendraRankingClient;
import software.amazon.awssdk.services.kendraranking.model.CreateRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.CreateRescoreExecutionPlanResponse;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Delay;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

public class CreateHandler extends BaseHandlerStd {

    private static final Constant STABILIZATION_DELAY = Constant.of()
        // Set the timeout to something silly/way too high, because
        // we already set the timeout in the schema https://github.com/aws-cloudformation/aws-cloudformation-resource-schema
        .timeout(Duration.ofDays(365L))
        // Set the delay to two minutes so the stabilization code only calls
        // DescribeRescoreExecutionPlan every one minute
        .delay(Duration.ofMinutes(2))
        .build();

    private Delay delay;

    private static final BiFunction<ResourceModel, ProxyClient<KendraRankingClient>, ResourceModel> EMPTY_CALL =
        (model, proxyClient) -> model;


  private ExecutionPlanArnBuilder executionPlanArnBuilder;

  public CreateHandler() {
    this(new ExecutionPlanPlanArn(), STABILIZATION_DELAY);
  }

  public CreateHandler(ExecutionPlanArnBuilder executionPlanArnBuilder, Delay delay) {
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

        if (request.getDesiredResourceTags() != null && !request.getDesiredResourceTags().isEmpty()) {
          model.setTags(Translator.transformTags(request.getDesiredResourceTags()));
        }

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
            // STEP 1 [create progress chain - required for resource creation]
            .then(progress ->
                proxy.initiate("AWS-KendraRanking-ExecutionPlan::Create", proxyClient, request.getDesiredResourceState(), callbackContext)
                    .translateToServiceRequest(Translator::translateToCreateRequest)
                    .makeServiceCall((createRerankingEndpointRequest, kendraRankingClientProxyClient)
                        -> createExecutionPlan(createRerankingEndpointRequest, kendraRankingClientProxyClient, logger))
                    .done(this::setId)
            )
            // STEP 2 stabilize
            .then(progress -> stabilize(proxy, proxyClient, progress, "AWS-KendraRanking-ExecutionPlan::PostCreateStabilize", logger))
            // STEP 3 [describe call/chain to return the resource model]
            .then(progress -> new ReadHandler(executionPlanArnBuilder).handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> setId(CreateRescoreExecutionPlanRequest createRescoreExecutionPlanRequest,
        CreateRescoreExecutionPlanResponse createRescoreExecutionPlanResponse,
        ProxyClient<KendraRankingClient> proxyClient,
        ResourceModel resourceModel,
        CallbackContext callbackContext) {

      resourceModel.setId(createRescoreExecutionPlanResponse.id());
      return ProgressEvent.progress(resourceModel, callbackContext);
    }


    private ProgressEvent<ResourceModel, CallbackContext> stabilize(
        final AmazonWebServicesClientProxy proxy,
        final ProxyClient<KendraRankingClient> proxyClient,
        final ProgressEvent<ResourceModel, CallbackContext> progress,
        String callGraph,
        final Logger logger) {
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
      DescribeRescoreExecutionPlanRequest describeRescoreExecutionPlanRequest = DescribeRescoreExecutionPlanRequest.builder()
          .id(model.getId())
          .build();
      DescribeRescoreExecutionPlanResponse describeRescoreExecutionPlanResponse =
          proxyClient.injectCredentialsAndInvokeV2(describeRescoreExecutionPlanRequest,
          proxyClient.client()::describeRescoreExecutionPlan);
      RescoreExecutionPlanStatus rerankingEndpointStatus = describeRescoreExecutionPlanResponse.status();
      if (RescoreExecutionPlanStatus.FAILED.equals(rerankingEndpointStatus)) {
        throw new CfnNotStabilizedException(ResourceModel.TYPE_NAME, model.getId());
      }
      boolean stabilized = RescoreExecutionPlanStatus.ACTIVE.equals(rerankingEndpointStatus);
      logger.log(String.format("%s [%s] create has stabilized: %s", ResourceModel.TYPE_NAME, model.getPrimaryIdentifier(), stabilized));
      return stabilized;
    }

    private CreateRescoreExecutionPlanResponse createExecutionPlan(
        final CreateRescoreExecutionPlanRequest createRerankingEndpointRequest,
        final ProxyClient<KendraRankingClient> proxyClient,
        final Logger logger) {
      CreateRescoreExecutionPlanResponse createRescoreExecutionPlanResponse;
      try {
        createRescoreExecutionPlanResponse = proxyClient.injectCredentialsAndInvokeV2(createRerankingEndpointRequest,
            proxyClient.client()::createRescoreExecutionPlan);
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
        throw new CfnGeneralServiceException(CREATE_EXECUTION_PLAN, e);
      }

    logger.log(String.format("%s successfully called CreateRescoreExecutionPlan and received ID %s. " +
        "Still need to stabilize.", ResourceModel.TYPE_NAME, createRescoreExecutionPlanResponse.id()));
    return createRescoreExecutionPlanResponse;
  }
}

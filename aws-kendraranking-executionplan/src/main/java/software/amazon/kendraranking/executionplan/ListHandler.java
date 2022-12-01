package software.amazon.kendraranking.executionplan;

import software.amazon.awssdk.services.kendraintelligentranking.KendraIntelligentRankingClient;
import software.amazon.awssdk.services.kendraintelligentranking.model.ListRescoreExecutionPlansRequest;
import software.amazon.awssdk.services.kendraintelligentranking.model.ListRescoreExecutionPlansResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ListHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<KendraIntelligentRankingClient> proxyClient,
        final Logger logger) {

        // STEP 1 [TODO: construct a body of a request]
        final ListRescoreExecutionPlansRequest listRescoreExecutionPlansRequest = Translator.translateToListRequest(request.getNextToken());

        // STEP 2 [TODO: make an api call]
        ListRescoreExecutionPlansResponse listRescoreExecutionPlansResponse = proxy.injectCredentialsAndInvokeV2(listRescoreExecutionPlansRequest,
            proxyClient.client()::listRescoreExecutionPlans);

        // STEP 3 [TODO: get a token for the next page]
        String nextToken = listRescoreExecutionPlansResponse.nextToken();

        // STEP 4 [TODO: construct resource models]
        // e.g. https://github.com/aws-cloudformation/aws-cloudformation-resource-providers-logs/blob/master/aws-logs-loggroup/src/main/java/software/amazon/logs/loggroup/ListHandler.java#L19-L21

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModels(Translator.translateFromListResponse(listRescoreExecutionPlansResponse))
            .nextToken(nextToken)
            .status(OperationStatus.SUCCESS)
            .build();
    }
}

package software.amazon.kendraranking.executionplan;

import software.amazon.awssdk.services.kendraranking.KendraRankingClient;
import software.amazon.awssdk.services.kendraranking.model.ListRescoreExecutionPlansRequest;
import software.amazon.awssdk.services.kendraranking.model.ListRescoreExecutionPlansResponse;
import software.amazon.awssdk.services.kendraranking.model.RescoreExecutionPlanSummary;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;


import java.time.Duration;
import java.util.Arrays;
import java.util.List;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase{

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<KendraRankingClient> proxyClient;

    @Mock
    KendraRankingClient sdkClient;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        sdkClient = mock(KendraRankingClient.class);
        proxyClient = MOCK_PROXY(proxy, sdkClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final ListHandler handler = new ListHandler();

        String id = "id";
        final ResourceModel model = ResourceModel
            .builder()
            .id(id)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        List<RescoreExecutionPlanSummary> summaryList =
            Arrays.asList(RescoreExecutionPlanSummary
                .builder()
                .id(id)
                .build());
        when(proxyClient.client().listRescoreExecutionPlans(any(ListRescoreExecutionPlansRequest.class)))
            .thenReturn(ListRescoreExecutionPlansResponse.builder().summaryItems(summaryList).build());

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).listRescoreExecutionPlans(any(ListRescoreExecutionPlansRequest.class));
    }
}

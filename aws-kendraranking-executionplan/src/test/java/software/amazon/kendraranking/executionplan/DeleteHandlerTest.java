package software.amazon.kendraranking.executionplan;

import java.time.Duration;
import software.amazon.awssdk.services.kendraintelligentranking.KendraIntelligentRankingClient;
import software.amazon.awssdk.services.kendraintelligentranking.model.ConflictException;
import software.amazon.awssdk.services.kendraintelligentranking.model.DeleteRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraintelligentranking.model.DeleteRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraintelligentranking.model.DescribeRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraintelligentranking.model.DescribeRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraintelligentranking.model.RescoreExecutionPlanStatus;
import software.amazon.awssdk.services.kendraintelligentranking.model.ResourceNotFoundException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Delay;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<KendraIntelligentRankingClient> proxyClient;

    @Mock
    KendraIntelligentRankingClient sdkClient;

    Delay testDelay = Constant.of().timeout(Duration.ofMinutes(1)).delay(Duration.ofMillis(1L)).build();

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        sdkClient = mock(KendraIntelligentRankingClient.class);
        proxyClient = MOCK_PROXY(proxy, sdkClient);
    }

    @AfterEach
    public void tear_down() {
        verify(sdkClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(sdkClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final DeleteHandler handler = new DeleteHandler(testDelay);

        String name = "name";
        String id = "id";
        final ResourceModel model = ResourceModel
            .builder()
            .name(name)
            .id(id)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder().status(RescoreExecutionPlanStatus.ACTIVE).build())
            .thenThrow(ResourceNotFoundException.builder().build());

        when(proxyClient.client().deleteRescoreExecutionPlan(any(DeleteRescoreExecutionPlanRequest.class)))
            .thenReturn(DeleteRescoreExecutionPlanResponse.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).deleteRescoreExecutionPlan(any(DeleteRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(2)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
    }

    @Test
    public void handleRequest_DeletingToNotFound() {
        final DeleteHandler handler = new DeleteHandler(testDelay);

        String name = "name";
        String id = "id";
        final ResourceModel model = ResourceModel
            .builder()
            .name(name)
            .id(id)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder().status(RescoreExecutionPlanStatus.ACTIVE).build())
            .thenReturn(DescribeRescoreExecutionPlanResponse
                .builder()
                .status(RescoreExecutionPlanStatus.DELETING)
                .build())
            .thenThrow(ResourceNotFoundException.builder().build());

        when(proxyClient.client().deleteRescoreExecutionPlan(any(DeleteRescoreExecutionPlanRequest.class)))
            .thenReturn(DeleteRescoreExecutionPlanResponse.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).deleteRescoreExecutionPlan(any(DeleteRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(3)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
    }

    @Test
    public void handleRequest_FailWith_ConflictException() {
        final DeleteHandler handler = new DeleteHandler(testDelay);

        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder().build());

        when(proxyClient.client().deleteRescoreExecutionPlan(any(DeleteRescoreExecutionPlanRequest.class)))
            .thenThrow(ConflictException.builder().build());

        final ResourceModel model = ResourceModel
            .builder()
            .name("name")
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        assertThrows(CfnResourceConflictException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });

        verify(proxyClient.client(), times(1)).deleteRescoreExecutionPlan(any(DeleteRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(1)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
    }
}

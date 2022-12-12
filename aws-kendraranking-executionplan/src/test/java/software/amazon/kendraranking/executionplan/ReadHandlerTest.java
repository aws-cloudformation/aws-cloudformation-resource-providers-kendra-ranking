package software.amazon.kendraranking.executionplan;

import java.time.Duration;
import java.util.Arrays;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kendraranking.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kendraranking.model.Tag;
import software.amazon.awssdk.services.kendraranking.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kendraranking.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.kendraranking.KendraRankingClient;
import software.amazon.awssdk.services.kendraranking.model.CapacityUnitsConfiguration;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraranking.model.RescoreExecutionPlanStatus;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<KendraRankingClient> proxyClient;

    @Mock
    KendraRankingClient sdkClient;


    TestExecutionArnBuilder testExecutionArnBuilder = new TestExecutionArnBuilder();

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        sdkClient = mock(KendraRankingClient.class);
        proxyClient = MOCK_PROXY(proxy, sdkClient);
    }

    @AfterEach
    public void tear_down() {
        verifyNoMoreInteractions(sdkClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final ReadHandler handler = new ReadHandler(testExecutionArnBuilder);

        String id = "testId";
        String name = "testName";

        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder()
                .id(id)
                .name(name)
                .status(RescoreExecutionPlanStatus.ACTIVE.toString())
                .capacityUnits(CapacityUnitsConfiguration.builder().rescoreCapacityUnits(10).build())
                .build());

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse.builder().build());

        final ResourceModel model = ResourceModel
            .builder()
            .id(id)
            .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        final ResourceModel expected = ResourceModel
            .builder()
            .id(id)
            .arn(testExecutionArnBuilder.build(request))
            .name(name)
            .capacityUnits(software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration.builder().rescoreCapacityUnits(10).build())
            .build();
        assertThat(response.getResourceModel()).isEqualTo(expected);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_Tags() {
        final ReadHandler handler = new ReadHandler(testExecutionArnBuilder);

        String id = "testId";
        String name = "testName";

        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder()
                .id(id)
                .name(name)
                .status(RescoreExecutionPlanStatus.ACTIVE.toString())
                .capacityUnits(CapacityUnitsConfiguration.builder().rescoreCapacityUnits(10).build())
                .build());

        String key = "key";
        String value = "value";
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse
                .builder()
                .tags(Arrays.asList(Tag.builder().key(key).value(value).build()))
                .build());

        final ResourceModel model = ResourceModel
            .builder()
            .id(id)
            .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        final ResourceModel expected = ResourceModel
            .builder()
            .id(id)
            .arn(testExecutionArnBuilder.build(request))
            .name(name)
            .tags(Arrays.asList(software.amazon.kendraranking.executionplan.Tag.builder().key(key).value(value).build()))
            .capacityUnits(software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration.builder().rescoreCapacityUnits(10).build())
            .build();
        assertThat(response.getResourceModel()).isEqualTo(expected);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_ThrowsResourceNotFoundException() {
        final ReadHandler handler = new ReadHandler(testExecutionArnBuilder);
        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenThrow(ResourceNotFoundException.builder().build());

        final ResourceModel model = ResourceModel
            .builder()
            .id("id")
            .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        assertThrows(CfnNotFoundException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });

        verify(proxyClient.client(), times(1)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
    }

    @Test
    public void handleRequest_HandlesGeneralServiceException() {
        final ReadHandler handler = new ReadHandler(testExecutionArnBuilder);

        String id = "testId";
        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenThrow(AwsServiceException.builder().build());

        final ResourceModel model = ResourceModel
            .builder()
            .id(id)
            .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        assertThrows(CfnGeneralServiceException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });
    }
}

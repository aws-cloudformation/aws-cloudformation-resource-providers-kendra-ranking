package software.amazon.kendraranking.executionplan;

import java.time.Duration;
import java.util.Arrays;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kendraranking.KendraRankingClient;
import software.amazon.awssdk.services.kendraranking.model.AccessDeniedException;
import software.amazon.awssdk.services.kendraranking.model.ConflictException;
import software.amazon.awssdk.services.kendraranking.model.CreateRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.CreateRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraranking.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kendraranking.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.kendraranking.model.RescoreExecutionPlanStatus;
import software.amazon.awssdk.services.kendraranking.model.ServiceQuotaExceededException;
import software.amazon.awssdk.services.kendraranking.model.ValidationException;
import software.amazon.cloudformation.exceptions.CfnAccessDeniedException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.exceptions.CfnResourceConflictException;
import software.amazon.cloudformation.exceptions.CfnServiceLimitExceededException;
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
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<KendraRankingClient> proxyClient;

    @Mock
    KendraRankingClient sdkClient;

    TestExecutionArnBuilder testExecutionArnBuilder = new TestExecutionArnBuilder();

    Delay testDelay = Constant.of().timeout(Duration.ofMinutes(1)).delay(Duration.ofMillis(1L)).build();


    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        sdkClient = mock(KendraRankingClient.class);
        proxyClient = MOCK_PROXY(proxy, sdkClient);
    }

    @AfterEach
    public void tear_down() {
        verify(sdkClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(sdkClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final CreateHandler handler = new CreateHandler(testExecutionArnBuilder, testDelay);
        software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration capacityUnitsConfiguration =
            software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration.builder()
            .rescoreCapacityUnits(10).build();
        String name = "testName";
        String description = "description";
        final ResourceModel model = ResourceModel
            .builder()
            .name(name)
            .description(description)
            .capacityUnits(capacityUnitsConfiguration)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        String id = "testId";
        when(proxyClient.client().createRescoreExecutionPlan(any(CreateRescoreExecutionPlanRequest.class)))
            .thenReturn(CreateRescoreExecutionPlanResponse.builder().id(id).build());

        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder()
                .id(id)
                .name(name)
                .status(RescoreExecutionPlanStatus.ACTIVE.toString())
                .capacityUnits(software.amazon.awssdk.services.kendraranking.model.CapacityUnitsConfiguration
                    .builder().rescoreCapacityUnits(10).build())
                .description(description)
                .build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        ResourceModel expectedResourceModel = ResourceModel
            .builder()
            .id(id)
            .arn(testExecutionArnBuilder.build(request))
            .name(name)
            .capacityUnits(capacityUnitsConfiguration)
            .description(description)

            .build();
        assertThat(response.getResourceModel()).isEqualTo(expectedResourceModel);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).createRescoreExecutionPlan(any(CreateRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(2)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessTransitionsFromCreatingToActive() {
        final CreateHandler handler = new CreateHandler(testExecutionArnBuilder, testDelay);

        String name = "testName";
        final ResourceModel model = ResourceModel
            .builder()
            .name(name)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();


        String id = "testId";
        when(proxyClient.client().createRescoreExecutionPlan(any(CreateRescoreExecutionPlanRequest.class)))
            .thenReturn(CreateRescoreExecutionPlanResponse.builder().id(id).build());
        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder()
                    .id(id)
                    .name(name)
                    .status(RescoreExecutionPlanStatus.CREATING)
                    .build(),
                DescribeRescoreExecutionPlanResponse.builder()
                    .id(id)
                    .name(name)
                    .status(RescoreExecutionPlanStatus.ACTIVE)
                    .build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse.builder().build());


        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        ResourceModel expectedResourceModel = ResourceModel
            .builder()
            .id(id)
            .arn(testExecutionArnBuilder.build(request))
            .capacityUnits(software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration.builder().rescoreCapacityUnits(0).build())
            .name(name)
            .build();
        assertThat(response.getResourceModel()).isEqualTo(expectedResourceModel);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).createRescoreExecutionPlan(any(CreateRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(3)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_FailWith_GeneralAwsServiceException() {
        final CreateHandler handler = new CreateHandler(testExecutionArnBuilder, testDelay);

        when(proxyClient.client().createRescoreExecutionPlan(any(CreateRescoreExecutionPlanRequest.class)))
            .thenThrow(AwsServiceException.builder().build());

        final ResourceModel model = ResourceModel
            .builder()
            .name("name")
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        assertThrows(CfnGeneralServiceException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });
    }

    @Test
    public void handleRequest_CreateExecutionPlanFailedAsynchronously() {
        final CreateHandler handler = new CreateHandler(testExecutionArnBuilder, testDelay);

        String name = "testName";
        final ResourceModel model = ResourceModel
            .builder()
            .name(name)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        String id = "testId";
        when(proxyClient.client().createRescoreExecutionPlan(any(CreateRescoreExecutionPlanRequest.class)))
            .thenReturn(CreateRescoreExecutionPlanResponse.builder().id(id).build());

        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder()
                .id(id)
                .name(name)
                .status(RescoreExecutionPlanStatus.FAILED.toString())
                .build());

        assertThrows(CfnNotStabilizedException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });
    }

    @Test
    public void handleRequest_FailWith_ConflictException() {
        final CreateHandler handler = new CreateHandler(testExecutionArnBuilder, testDelay);

        when(proxyClient.client().createRescoreExecutionPlan(any(CreateRescoreExecutionPlanRequest.class)))
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
    }

    @Test
    public void handleRequest_Tags() {
        final CreateHandler handler = new CreateHandler(testExecutionArnBuilder, testDelay);

        String name = "testName";
        String key = "key";
        String value = "value";
        final ResourceModel model = ResourceModel
            .builder()
            .name(name)
            .tags(Arrays.asList(Tag.builder().key(key).value(value).build()))
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        String id = "testId";
        when(proxyClient.client().createRescoreExecutionPlan(any(CreateRescoreExecutionPlanRequest.class)))
            .thenReturn(CreateRescoreExecutionPlanResponse.builder().id(id).build());

        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder()
                .id(id)
                .name(name)
                .status(RescoreExecutionPlanStatus.ACTIVE.toString())
                .build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse
                .builder()
                .tags(Arrays.asList(software.amazon.awssdk.services.kendraranking.model.Tag
                    .builder().key(key).value(value).build()))
                .build());


        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        ResourceModel expectedResourceModel = ResourceModel
            .builder()
            .id(id)
            .arn(testExecutionArnBuilder.build(request))
            .name(name)
            .capacityUnits(software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration.builder().rescoreCapacityUnits(0).build())
            .tags(Arrays.asList(Tag.builder().key(key).value(value).build()))
            .build();
        assertThat(response.getResourceModel()).isEqualTo(expectedResourceModel);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).createRescoreExecutionPlan(any(CreateRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(2)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_FailWith_QuoteException() {
        final CreateHandler handler = new CreateHandler(testExecutionArnBuilder, testDelay);

        when(proxyClient.client().createRescoreExecutionPlan(any(CreateRescoreExecutionPlanRequest.class)))
            .thenThrow(ServiceQuotaExceededException.builder().build());

        final ResourceModel model = ResourceModel
            .builder()
            .name("name")
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        assertThrows(CfnServiceLimitExceededException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });
    }

    @Test
    public void handleRequest_FailWith_InvalidException() {
        final CreateHandler handler = new CreateHandler(testExecutionArnBuilder, testDelay);

        when(proxyClient.client().createRescoreExecutionPlan(any(CreateRescoreExecutionPlanRequest.class)))
            .thenThrow(ValidationException.builder().build());

        final ResourceModel model = ResourceModel
            .builder()
            .name("name")
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        assertThrows(CfnInvalidRequestException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });
    }

    @Test
    public void handleRequest_FailWith_AccessException() {
        final CreateHandler handler = new CreateHandler(testExecutionArnBuilder, testDelay);

        when(proxyClient.client().createRescoreExecutionPlan(any(CreateRescoreExecutionPlanRequest.class)))
            .thenThrow(AccessDeniedException.builder().build());

        final ResourceModel model = ResourceModel
            .builder()
            .name("name")
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        assertThrows(CfnAccessDeniedException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });
    }

}

package software.amazon.kendraranking.executionplan;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import software.amazon.awssdk.services.kendraranking.KendraRankingClient;
import software.amazon.awssdk.services.kendraranking.model.ConflictException;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraranking.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kendraranking.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.kendraranking.model.RescoreExecutionPlanStatus;
import software.amazon.awssdk.services.kendraranking.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kendraranking.model.ServiceQuotaExceededException;
import software.amazon.awssdk.services.kendraranking.model.Tag;
import software.amazon.awssdk.services.kendraranking.model.TagResourceRequest;
import software.amazon.awssdk.services.kendraranking.model.TagResourceResponse;
import software.amazon.awssdk.services.kendraranking.model.UntagResourceRequest;
import software.amazon.awssdk.services.kendraranking.model.UntagResourceResponse;
import software.amazon.awssdk.services.kendraranking.model.UpdateRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.UpdateRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraranking.model.ValidationException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

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
        verifyNoMoreInteractions(sdkClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final UpdateHandler handler = new UpdateHandler(testExecutionArnBuilder, testDelay);

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

        when(proxyClient.client().updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class)))
            .thenReturn(UpdateRescoreExecutionPlanResponse.builder().build());
        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder()
                .id(id)
                .name(name)
                .status(RescoreExecutionPlanStatus.ACTIVE.toString())
                .build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse
                .builder()
                .tags((java.util.Collection<Tag>) null)
                .build());
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);

        final ResourceModel expectedModel = ResourceModel
            .builder()
            .id(id)
            .arn(testExecutionArnBuilder.build(request))
            .name(name)
            .capacityUnits(software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration.builder().rescoreCapacityUnits(0).build())
            .build();
        assertThat(response.getResourceModel()).isEqualTo(expectedModel);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(3)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(sdkClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleRequest_UpdatingToActive() {
        final UpdateHandler handler = new UpdateHandler(testExecutionArnBuilder, testDelay);

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

        when(proxyClient.client().updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class)))
            .thenReturn(UpdateRescoreExecutionPlanResponse.builder().build());

        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(
                DescribeRescoreExecutionPlanResponse.builder()
                    .id(id)
                    .name(name)
                    .status(RescoreExecutionPlanStatus.ACTIVE.toString())
                    .build(),
                DescribeRescoreExecutionPlanResponse.builder()
                    .id(id)
                    .name(name)
                    .status(RescoreExecutionPlanStatus.UPDATING.toString())
                    .build(),
                DescribeRescoreExecutionPlanResponse
                    .builder()
                    .id(id)
                    .name(name)
                    .status(RescoreExecutionPlanStatus.ACTIVE.toString())
                    .build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);

        final ResourceModel expectedModel = ResourceModel
            .builder()
            .id(id)
            .arn(testExecutionArnBuilder.build(request))
            .capacityUnits(software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration.builder().rescoreCapacityUnits(0).build())
            .name(name)
            .build();
        assertThat(response.getResourceModel()).isEqualTo(expectedModel);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(4)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(sdkClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleRequest_FailWith_ConflictException() {
        final UpdateHandler handler = new UpdateHandler(testExecutionArnBuilder, testDelay);

        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse
                .builder()
                .build());
        when(proxyClient.client().updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class)))
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
        verify(proxyClient.client(), times(1)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(1)).updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class));
        verify(sdkClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleRequest_AddNewTags() {
        final UpdateHandler handler = new UpdateHandler(testExecutionArnBuilder, testDelay);

        String name = "name";
        String id = "id";
        String key = "key";
        String value = "value";
        List<software.amazon.kendraranking.executionplan.Tag> tags =
            Arrays.asList(software.amazon.kendraranking.executionplan.Tag.builder().key(key).value(value).build());
        final ResourceModel model = ResourceModel
            .builder()
            .id(id)
            .name(name)
            .tags(tags)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        when(proxyClient.client().updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class)))
            .thenReturn(UpdateRescoreExecutionPlanResponse.builder().build());
        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder()
                .id(id)
                .name(name)
                .status(RescoreExecutionPlanStatus.ACTIVE.toString())
                .build());

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse
                .builder()
                .tags(Arrays.asList(Tag
                    .builder().key(key).value(value).build()))
                .build());
        when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
            .thenReturn(TagResourceResponse.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        final ResourceModel expectedModel = ResourceModel
            .builder()
            .id(id)
            .arn(testExecutionArnBuilder.build(request))
            .capacityUnits(software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration.builder().rescoreCapacityUnits(0).build())
            .name(name)
            .tags(tags)
            .build();
        assertThat(response.getResourceModel()).isEqualTo(expectedModel);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(3)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client(), times(1)).tagResource(any(TagResourceRequest.class));
        verify(sdkClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleRequest_RemoveTags() {
        final UpdateHandler handler = new UpdateHandler(testExecutionArnBuilder, testDelay);

        String name = "name";
        String id = "id";
        final ResourceModel model = ResourceModel
            .builder()
            .id(id)
            .name(name)
            .build();

        String key = "key";
        String value = "value";
        final ResourceModel prevModel = ResourceModel
            .builder()
            .tags(Arrays.asList(software.amazon.kendraranking.executionplan.Tag.builder().key(key).value(value).build()))
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .previousResourceState(prevModel)
            .build();

        when(proxyClient.client().updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class)))
            .thenReturn(UpdateRescoreExecutionPlanResponse.builder().build());
        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder()
                .id(id)
                .name(name)
                .status(RescoreExecutionPlanStatus.ACTIVE.toString())
                .build());

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse.builder().build());
        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
            .thenReturn(UntagResourceResponse.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        final ResourceModel expectedModel = ResourceModel
            .builder()
            .id(id)
            .arn(testExecutionArnBuilder.build(request))
            .name(name)
            .capacityUnits(software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration.builder().rescoreCapacityUnits(0).build())
            .build();
        assertThat(response.getResourceModel()).isEqualTo(expectedModel);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(3)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client(), times(1)).untagResource(any(UntagResourceRequest.class));
        verify(sdkClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleRequest_AddAndRemoveTags() {
        final UpdateHandler handler = new UpdateHandler(testExecutionArnBuilder, testDelay);

        String name = "name";
        String id = "id";
        String keyAdd = "keyAdd";
        String valueAdd = "valueAdd";
        List<software.amazon.kendraranking.executionplan.Tag> tagsToAdd = Arrays.asList(software.amazon.kendraranking.executionplan.Tag.builder().key(keyAdd).value(valueAdd).build());
        final ResourceModel model = ResourceModel
            .builder()
            .id(id)
            .name(name)
            .tags(tagsToAdd)
            .build();

        String keyRemove = "keyRemove";
        String valueRemove = "valueRemove";
        final ResourceModel prevModel = ResourceModel
            .builder()
            .tags(Arrays.asList(software.amazon.kendraranking.executionplan.Tag.builder().key(keyRemove).value(valueRemove).build()))
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .previousResourceState(prevModel)
            .build();

        when(proxyClient.client().updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class)))
            .thenReturn(UpdateRescoreExecutionPlanResponse.builder().build());
        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder()
                .id(id)
                .name(name)
                .status(RescoreExecutionPlanStatus.ACTIVE.toString())
                .build());

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
            .thenReturn(ListTagsForResourceResponse
                .builder()
                .tags(Arrays.asList(Tag
                    .builder().key(keyAdd).value(valueAdd).build()))
                .build());

        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
            .thenReturn(UntagResourceResponse.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        final ResourceModel expectedModel = ResourceModel
            .builder()
            .id(id)
            .arn(testExecutionArnBuilder.build(request))
            .capacityUnits(software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration.builder().rescoreCapacityUnits(0).build())
            .name(name)
            .tags(tagsToAdd)
            .build();
        assertThat(response.getResourceModel()).isEqualTo(expectedModel);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(3)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client(), times(1)).tagResource(any(TagResourceRequest.class));
        verify(proxyClient.client(), times(1)).untagResource(any(UntagResourceRequest.class));
        verify(sdkClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleRequest_FailWith_TagResourceThrowsException() {
        final UpdateHandler handler = new UpdateHandler(testExecutionArnBuilder, testDelay);

        String name = "name";
        String id = "id";
        String key = "key";
        String value = "value";
        List<software.amazon.kendraranking.executionplan.Tag> tags = Arrays.asList(
            software.amazon.kendraranking.executionplan.Tag.builder().key(key).value(value).build());
        final ResourceModel model = ResourceModel
            .builder()
            .id(id)
            .name(name)
            .tags(tags)
            .build();

        when(proxyClient.client().updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class)))
            .thenReturn(UpdateRescoreExecutionPlanResponse.builder().build());
        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder()
                .id(id)
                .name(name)
                .status(RescoreExecutionPlanStatus.ACTIVE.toString())
                .build());

        when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
            .thenThrow(ValidationException.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        assertThrows(CfnInvalidRequestException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });

        verify(proxyClient.client(), times(1)).updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(2)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(sdkClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleRequest_FailWith_UntagResourceThrowsException() {
        final UpdateHandler handler = new UpdateHandler(testExecutionArnBuilder, testDelay);

        String name = "name";
        String id = "id";
        final ResourceModel model = ResourceModel
            .builder()
            .id(id)
            .name(name)
            .build();

        final ResourceModel prevModel = ResourceModel
            .builder()
            .tags(Arrays.asList(software.amazon.kendraranking.executionplan.Tag.builder().key("key").value("value").build()))
            .build();

        when(proxyClient.client().updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class)))
            .thenReturn(UpdateRescoreExecutionPlanResponse.builder().build());
        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse.builder()
                .id(id)
                .name(name)
                .status(RescoreExecutionPlanStatus.ACTIVE.toString())
                .build());

        when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
            .thenThrow(ValidationException.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .previousResourceState(prevModel)
            .build();

        assertThrows(CfnInvalidRequestException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });

        verify(proxyClient.client(), times(1)).updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(2)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(sdkClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleRequest_FailWith_QuotaException() {
        final UpdateHandler handler = new UpdateHandler(testExecutionArnBuilder, testDelay);

        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenReturn(DescribeRescoreExecutionPlanResponse
                .builder()
                .build());
        when(proxyClient.client().updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class)))
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
        verify(proxyClient.client(), times(1)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(proxyClient.client(), times(1)).updateRescoreExecutionPlan(any(UpdateRescoreExecutionPlanRequest.class));
        verify(sdkClient, atLeastOnce()).serviceName();
    }

    @Test
    public void handleRequest_FailWith_NotFound() {
        final UpdateHandler handler = new UpdateHandler(testExecutionArnBuilder, testDelay);

        when(proxyClient.client().describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class)))
            .thenThrow(ResourceNotFoundException.builder().build());

        final ResourceModel model = ResourceModel
            .builder()
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        assertThrows(CfnNotFoundException.class, () -> {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        });
        verify(proxyClient.client(), times(1)).describeRescoreExecutionPlan(any(DescribeRescoreExecutionPlanRequest.class));
        verify(sdkClient, atLeastOnce()).serviceName();
    }
}

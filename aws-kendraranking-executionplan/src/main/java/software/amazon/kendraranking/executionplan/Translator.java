package software.amazon.kendraranking.executionplan;

import software.amazon.awssdk.services.kendraranking.model.CapacityUnitsConfiguration;
import software.amazon.awssdk.services.kendraranking.model.CreateRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.DeleteRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanRequest;
import software.amazon.awssdk.services.kendraranking.model.DescribeRescoreExecutionPlanResponse;
import software.amazon.awssdk.services.kendraranking.model.ListRescoreExecutionPlansRequest;
import software.amazon.awssdk.services.kendraranking.model.ListRescoreExecutionPlansResponse;
import software.amazon.awssdk.services.kendraranking.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.kendraranking.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.kendraranking.model.Tag;
import software.amazon.awssdk.services.kendraranking.model.TagResourceRequest;
import software.amazon.awssdk.services.kendraranking.model.UntagResourceRequest;
import software.amazon.awssdk.services.kendraranking.model.UpdateRescoreExecutionPlanRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is a centralized placeholder for
 *  - api request construction
 *  - object translation to/from aws sdk
 *  - resource model construction for read/list handlers
 */

public class Translator {
  /**
   * Request to create a resource
   * @param model resource model
   * @return awsRequest the aws service request to create a resource
   */
  static CreateRescoreExecutionPlanRequest translateToCreateRequest(final ResourceModel model) {
    final CreateRescoreExecutionPlanRequest.Builder builder = CreateRescoreExecutionPlanRequest
        .builder()
        .name(model.getName())
        .capacityUnits(translateToCapacityUnitsConfiguration(model.getCapacityUnits()))
        .description(model.getDescription());
    builder.tags(ListConverter.toSdk(model.getTags(), x -> Tag.builder().key(x.getKey()).value(x.getValue()).build()));
    return builder.build();
  }

  /**
   * Request to read a resource
   * @param model resource model
   * @return awsRequest the aws service request to describe a resource
   */
  static DescribeRescoreExecutionPlanRequest translateToReadRequest(final ResourceModel model) {
    final DescribeRescoreExecutionPlanRequest describeRescoreExecutionPlanRequest = DescribeRescoreExecutionPlanRequest.builder()
        .id(model.getId())
        .build();
    return describeRescoreExecutionPlanRequest;
  }

  /**
   * Translates resource object from sdk into a resource model
   * @param describeRescoreExecutionPlanResponse the aws service describe resource response
   * @return model resource model
   */
  static ResourceModel translateFromReadResponse(final DescribeRescoreExecutionPlanResponse describeRescoreExecutionPlanResponse,
      ListTagsForResourceResponse listTagsForResourceResponse,
      String arn) {
    // e.g. https://github.com/aws-cloudformation/aws-cloudformation-resource-providers-logs/blob/2077c92299aeb9a68ae8f4418b5e932b12a8b186/aws-logs-loggroup/src/main/java/com/aws/logs/loggroup/Translator.java#L58-L73
    ResourceModel.ResourceModelBuilder builder = ResourceModel.builder()
        .id(describeRescoreExecutionPlanResponse.id())
        .arn(arn)
        .name(describeRescoreExecutionPlanResponse.name())
        .capacityUnits(translateFromCapacityUnitsConfiguration(describeRescoreExecutionPlanResponse.capacityUnits()))
        .description(describeRescoreExecutionPlanResponse.description());

    List<software.amazon.kendraranking.executionplan.Tag> tags = ListConverter.toModel(listTagsForResourceResponse.tags(),
        x -> software.amazon.kendraranking.executionplan.Tag.builder().key(x.key()).value(x.value()).build());
    builder.tags(tags);

    return builder.build();
  }

  static software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration translateFromCapacityUnitsConfiguration(
      CapacityUnitsConfiguration capacityUnitsConfiguration) {

    if (capacityUnitsConfiguration != null) {
      return software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration
          .builder()
          .rescoreCapacityUnits(capacityUnitsConfiguration.rescoreCapacityUnits())
          .build();
    } else {
      // Null equivalent.
      return software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration
          .builder()
          .rescoreCapacityUnits(0)
          .build();
    }
  }


  /**
   * Request to delete a resource
   * @param model resource model
   * @return awsRequest the aws service request to delete a resource
   */
  static DeleteRescoreExecutionPlanRequest translateToDeleteRequest(final ResourceModel model) {
    final DeleteRescoreExecutionPlanRequest deleteRescoreExecutionPlanRequest = DeleteRescoreExecutionPlanRequest
        .builder()
        .id(model.getId())
        .build();
    return deleteRescoreExecutionPlanRequest;
  }

  /**
   * Request to list resources
   * @param nextToken token passed to the aws service list resources request
   * @return awsRequest the aws service request to list resources within aws account
   */
  static ListRescoreExecutionPlansRequest translateToListRequest(final String nextToken) {
    final ListRescoreExecutionPlansRequest listRescoreExecutionPlansRequest = ListRescoreExecutionPlansRequest
        .builder()
        .nextToken(nextToken)
        .build();
    return listRescoreExecutionPlansRequest;
  }

  /**
   * Translates resource objects from sdk into a resource model (primary identifier only)
   * @param listRescoreExecutionPlansResponse the aws service describe resource response
   * @return list of resource models
   */
  static List<ResourceModel> translateFromListResponse(final ListRescoreExecutionPlansResponse listRescoreExecutionPlansResponse) {
    // e.g. https://github.com/aws-cloudformation/aws-cloudformation-resource-providers-logs/blob/2077c92299aeb9a68ae8f4418b5e932b12a8b186/aws-logs-loggroup/src/main/java/com/aws/logs/loggroup/Translator.java#L75-L82
    return streamOfOrEmpty(listRescoreExecutionPlansResponse.summaryItems())
        .map(resource -> ResourceModel.builder()
            .id(resource.id())
            .build())
        .collect(Collectors.toList());
  }

  private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
    return Optional.ofNullable(collection)
        .map(Collection::stream)
        .orElseGet(Stream::empty);
  }

  static ListTagsForResourceRequest translateToListTagsRequest(final String arn) {
    return ListTagsForResourceRequest
        .builder()
        .resourceARN(arn)
        .build();
  }

  static UpdateRescoreExecutionPlanRequest translateToUpdateRequest(final ResourceModel currModel,
      final ResourceModel prevModel) throws TranslatorValidationException {
    // Null equivalents for partial updates.
    String description = currModel.getDescription() == null ? "" : currModel.getDescription();
    String name = currModel.getName() == null ? "" : currModel.getName();

    final UpdateRescoreExecutionPlanRequest.Builder builder = UpdateRescoreExecutionPlanRequest
        .builder()
        .id(currModel.getId())
        .name(name)
        .description(description)
        .capacityUnits(translateToCapacityUnitsConfiguration(currModel.getCapacityUnits()));
    return builder.build();
  }

  static CapacityUnitsConfiguration translateToCapacityUnitsConfiguration(
      software.amazon.kendraranking.executionplan.CapacityUnitsConfiguration modelCapacityUnitsConfiguration) {

    if (modelCapacityUnitsConfiguration != null) {
      return CapacityUnitsConfiguration
          .builder()
          .rescoreCapacityUnits(modelCapacityUnitsConfiguration.getRescoreCapacityUnits())
          .build();
    } else {
      // Null equivalent.
      return CapacityUnitsConfiguration
          .builder()
          .rescoreCapacityUnits(0)
          .build();
    }
  }

  /**
   * Request to add tags to a resource
   * @param tags resource model
   * @return awsRequest the aws service request to create a resource
   */
  static UntagResourceRequest translateToUntagResourceRequest(Set<software.amazon.kendraranking.executionplan.Tag> tags,
      String arn) {
    return UntagResourceRequest
        .builder()
        .resourceARN(arn)
        .tagKeys(tags.stream().map(x -> x.getKey()).collect(Collectors.toList()))
        .build();
  }

  /**
   * Request to add tags to a resource
   * @param tags resource model
   * @return awsRequest the aws service request to create a resource
   */
  static TagResourceRequest translateToTagResourceRequest(Set<software.amazon.kendraranking.executionplan.Tag> tags,
      String arn) {
    return TagResourceRequest
        .builder()
        .resourceARN(arn)
        .tags(tags.stream().map(x -> Tag
                .builder()
                .key(x.getKey())
                .value(x.getValue()).build())
            .collect(Collectors.toList()))
        .build();
  }

  /**
   * Overloaded converter method to convert Map<String, String> to Collection<Tag>
   * where Tag is SDK object and Tags is RPDK object
   */
  static List<software.amazon.kendraranking.executionplan.Tag> transformTags(final Map<String, String> tags) {
    if (tags == null) return null;
    final List<software.amazon.kendraranking.executionplan.Tag> tags_collection =
        tags.entrySet().stream()
            .map(e -> software.amazon.kendraranking.executionplan.Tag.builder().key(e.getKey()).value(e.getValue()).build())
            .collect(Collectors.toList());
    return tags_collection;
  }
}

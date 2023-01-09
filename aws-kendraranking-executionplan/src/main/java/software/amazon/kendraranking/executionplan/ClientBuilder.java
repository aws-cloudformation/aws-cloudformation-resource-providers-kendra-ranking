package software.amazon.kendraranking.executionplan;

import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kendraranking.KendraRankingClient;
import software.amazon.cloudformation.LambdaWrapper;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;

public class ClientBuilder {

//  private static final BackoffStrategy KENDRA_RANKING_BACKOFF_THROTTLING_STRATEGY =
//      EqualJitterBackoffStrategy.builder()
//          .baseDelay(Duration.ofMinutes(1))
//          .maxBackoffTime(Duration.ofMinutes(2))
//          .build();
//
//  private static final RetryPolicy KENDRA_RANKING_RETRY_POLICY =
//      RetryPolicy.builder()
//          .numRetries(3)
//          .retryCondition(RetryCondition.defaultRetryCondition())
//          .throttlingBackoffStrategy(KENDRA_RANKING_BACKOFF_THROTTLING_STRATEGY)
//          .build();
  public static KendraRankingClient getClient(String region) {
    return KendraRankingClient.builder().httpClient(LambdaWrapper.HTTP_CLIENT)
        // TODO remove region after opensearch launch
        .endpointOverride(URI.create("https://kendra-ranking." + region + ".api.aws"))
        .overrideConfiguration(ClientOverrideConfiguration.builder()
            //.retryPolicy(KENDRA_RANKING_RETRY_POLICY)
            .build())
        .region(REGIONS.get(region))
        .build();
  }

  // TODO remove after opensearch launch
  static final Map<String, Region> REGIONS = ImmutableMap.<String, Region>builder()
      .put("us-west-2", Region.US_WEST_2)
      .put("us-east-1", Region.US_EAST_1)
      .put("us-east-2", Region.US_EAST_2)
      .put("eu-west-1", Region.EU_WEST_1)
      .put("ap-south-1", Region.AP_SOUTH_1)
      .put("ap-southeast-1", Region.AP_SOUTHEAST_1)
      .put("ap-southeast-2", Region.AP_SOUTHEAST_2)
      .put("ca-central-1", Region.CA_CENTRAL_1)
      .build();



}

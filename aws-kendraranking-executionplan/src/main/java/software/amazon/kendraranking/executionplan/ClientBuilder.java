package software.amazon.kendraranking.executionplan;

import java.net.URI;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kendraranking.KendraRankingClient;
import software.amazon.cloudformation.LambdaWrapper;

public class ClientBuilder {
  public static KendraRankingClient getClient() {
    return KendraRankingClient.builder().httpClient(LambdaWrapper.HTTP_CLIENT)
        .endpointOverride(URI.create("https://kendra-ranking.us-west-2.api.aws"))
        .region(Region.US_WEST_2)
        .build();
  }
}

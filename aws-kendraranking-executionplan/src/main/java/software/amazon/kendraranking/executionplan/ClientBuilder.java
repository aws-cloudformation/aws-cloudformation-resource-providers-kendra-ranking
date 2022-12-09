package software.amazon.kendraranking.executionplan;

import software.amazon.awssdk.services.kendraranking.KendraRankingClient;
import software.amazon.cloudformation.LambdaWrapper;

public class ClientBuilder {
  public static KendraRankingClient getClient() {
    return KendraRankingClient.builder().httpClient(LambdaWrapper.HTTP_CLIENT).build();
  }
}

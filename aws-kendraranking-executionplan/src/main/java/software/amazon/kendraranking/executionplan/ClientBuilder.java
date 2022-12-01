package software.amazon.kendraranking.executionplan;

import software.amazon.awssdk.services.kendraintelligentranking.KendraIntelligentRankingClient;
import software.amazon.cloudformation.LambdaWrapper;

public class ClientBuilder {
  public static KendraIntelligentRankingClient getClient() {
    return KendraIntelligentRankingClient.builder().httpClient(LambdaWrapper.HTTP_CLIENT).build();
  }
}

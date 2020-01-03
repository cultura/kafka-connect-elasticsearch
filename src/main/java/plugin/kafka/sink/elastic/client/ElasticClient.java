/*
    Copyright 2019 Cultura (SOCULTUR).

    This file is part of kafka-connect-elasticsearch.

    kafka-connect-elasticsearch is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    kafka-connect-elasticsearch is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with Foobar.  If not, see <https://www.gnu.org/licenses/>.

    contact: team.api.support@cultura.fr
 */

package plugin.kafka.sink.elastic.client;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Builder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.kafka.connect.errors.ConnectException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import plugin.kafka.sink.elastic.configs.ElasticConfig;
import plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig;

@Builder
@Slf4j
public class ElasticClient {

  private static final Pattern ELASTIC_HOST_PATTERN = Pattern
      .compile("([htps]+):\\/\\/([^:]+):(\\d+).*");
  private static final long ONE_MINUTE_IN_MILLIS = 60_000L;

  @Setter
  private Long lastRestart;
  private String indexName;
  private int shardNumber;
  private int replica;
  private ElasticConfig elasticConfig;
  private RestHighLevelClient client;
  private ElasticsearchSinkConnectorConfig config;

  public void initClient(){
    this.client =  ClientBuilder.initClient(elasticConfig, config);
  }

  public synchronized void createIndex(String mapping) {
    log.debug("---------------------------> Check if index {} exists", indexName);
    try {
      boolean exists = this.client.indices()
          .exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
      if (!exists) {
        log.debug("elastic ---------------------------> Index does not exists, creating index {}",
            indexName);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.settings(Settings.builder()
            .put("index.number_of_shards", this.shardNumber)
            .put("index.number_of_replicas", this.replica));
        createIndexRequest.mapping(mapping, XContentType.JSON);
        this.client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        log.debug("elastic ---------------------------> Index {} created successfully", indexName);
      }
    }catch (IOException | RuntimeException e) {
      log.error("elastic ---------------------------> Unable to find or create index {}", indexName, e);
    }
  }

  public BulkResponse bulk(BulkRequest bulkRequest) throws IOException {
    return client.bulk(bulkRequest, RequestOptions.DEFAULT);
  }

  public static HttpHost buildElasticHost(String url) {
    Matcher matcher = ELASTIC_HOST_PATTERN.matcher(url);
    if (matcher.matches()) {
      return new HttpHost(matcher.group(2), Integer.parseInt(matcher.group(3)), matcher.group(1));
    } else {
      throw new ConnectException(
          "The url " + url + " doesn't match the host pattern : http(s)://hostname:port/...");
    }
  }

  public void close(){
    try {
      this.client.close();
    } catch (IOException e) {
      log.error("Unable to close elastic rest client properly", e);
    }
  }

  /**
   * After a long idle time, the client loose web socket connection.
   * The client has to be restarting to work again.
   */
  public synchronized void resetClient(){
    if (this.lastRestart + ONE_MINUTE_IN_MILLIS <= System.currentTimeMillis()) {
      log.info("elastic ---------------------------> Restarting elastic client");
      this.close();
      this.initClient();
      this.lastRestart = System.currentTimeMillis();
    }
  }
}

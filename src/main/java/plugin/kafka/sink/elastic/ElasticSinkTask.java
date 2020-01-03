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

package plugin.kafka.sink.elastic;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import plugin.kafka.sink.elastic.client.ElasticClient;
import plugin.kafka.sink.elastic.configs.ElasticConfig;
import plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig;
import plugin.kafka.sink.elastic.configs.SharedContext;
import plugin.kafka.sink.elastic.health.TaskHealth;
import plugin.kafka.sink.elastic.utils.DataConverter;
import plugin.kafka.sink.elastic.utils.Mapping;

@Slf4j
public class ElasticSinkTask extends SinkTask {

  private static final int RANDOM_BOUND = 1_000_000;
  @Getter
  private Orchestrator orchestrator;
  private String indexName;
  private boolean isIndexCreated;
  @Setter
  private boolean isRecordKeyId;
  @Setter
  private Time time;
  private Random random = new Random();
  @Setter
  private ElasticClient client;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> props) {
    log.info("---------------------------> start task");
    try {
      this.time = new SystemTime();

      ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
      long flushTimeoutMS = config.getLong(ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG);
      int readTimeoutMS = config.getInt(ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG);
      this.indexName = config.getString(ElasticsearchSinkConnectorConfig.INDEX_NAME);
      isRecordKeyId = config.getBoolean(ElasticsearchSinkConnectorConfig.INDEX_USE_KEY_AS_ID);
      int shardNumber = config.getInt(ElasticsearchSinkConnectorConfig.NUMBER_OF_SHARD);
      int replica = config.getInt(ElasticsearchSinkConnectorConfig.NUMBER_OF_REPLICA);
      int maxBufferedRecords = config.getInt(ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG);
      long bufferLingerMs = config.getLong(ElasticsearchSinkConnectorConfig.BUFFERED_LINGER_MS_CONFIG);
      int batchSize = config.getInt(ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG);
      long lingerMs = config.getLong(ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG);
      int maxInFlightRequests = config.getInt(ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG);
      long retryBackoffMs = config.getLong(ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG);
      int maxRetries = config.getInt(ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG);
      Password password = config.getPassword(ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG);
      ElasticConfig elasticConfig = ElasticConfig.builder()
          .hostUrls(config.getList(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG))
          .readTimeout(readTimeoutMS)
          .userName(config.getString(ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG))
          .password(password != null ? password.value() : null)
          .build();

      this.isIndexCreated = false;

      this.client = ElasticClient.builder()
          .indexName(indexName)
          .shardNumber(shardNumber)
          .replica(replica)
          .elasticConfig(elasticConfig)
          .lastRestart(time.milliseconds())
          .config(config)
          .build();
      this.client.initClient();
      log.info("---------------------------> client successfully started");

      SharedContext sharedContext = SharedContext.builder()
          .client(client)
          .taskHealth(new TaskHealth())
          .lingerMs(lingerMs)
          .maxInFlightRequests(maxInFlightRequests)
          .batchSize(batchSize)
          .maxRetries(maxRetries)
          .retryBackoffMs(retryBackoffMs)
          .bufferLingerMs(bufferLingerMs)
          .readTimeoutMS(readTimeoutMS)
          .flushTimeoutMS(flushTimeoutMS)
          .maxBufferedRecords(maxBufferedRecords)
          .build();
      sharedContext.init();

      this.orchestrator = Orchestrator.builder()
          .sharedContext(sharedContext)
          .build();
      this.orchestrator.initOrchestrator();
      log.info("---------------------------> orchestrator successfully started");

    } catch (Exception e){
      log.error("---------------------------> error while starting task", e);
      throw new ConnectException(e);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {

      createIndexIfNeeded(record);

      String key = DataConverter.convertKey(record);
      String payload = DataConverter.getPayload(record);
      if (payload!=null) {
        this.orchestrator.addRequest(new IndexRequest(indexName).id(generateDocId(key))
            .source(payload, XContentType.JSON));
      }
    }
  }

  protected String generateDocId(String key) {
    String docId;
    if (isRecordKeyId && StringUtils.isNotBlank(key)){
      docId = key;
    } else {
      docId = time.milliseconds() + "-" + random.nextInt(RANDOM_BOUND);
    }
    return docId;
  }

  private synchronized void createIndexIfNeeded(SinkRecord record) {
    if (!isIndexCreated) {
      JsonNode jsonNode = Mapping.inferMapping(record.valueSchema());
      client.createIndex(jsonNode.toString());
      this.isIndexCreated = true;
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    this.orchestrator.flush();
  }

  @Override
  public void stop() {
    log.info("---------------------------> Stopping task");
    if (this.orchestrator != null) {
      this.orchestrator.stop();
    }
  }

}
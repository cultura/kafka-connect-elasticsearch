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
    along with kafka-connect-elasticsearch.  If not, see <https://www.gnu.org/licenses/>.

    contact: team.api.support@cultura.fr
 */

package plugin.kafka.sink.elastic.configs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.action.index.IndexRequest;
import plugin.kafka.sink.elastic.client.ElasticClient;
import plugin.kafka.sink.elastic.health.TaskHealth;
import plugin.kafka.sink.elastic.workers.BulkProcessorTask;

/**
 * Contain the context used by the connector to run properly. Every classes use it to have access
 * to the configuration values, the requests queue, the state of the task and the {@link ElasticClient}
 */
@Builder
@Getter
public class SharedContext {

  @Setter
  private ElasticClient client;
  private TaskHealth taskHealth;

  private long lingerMs;
  private int maxInFlightRequests;
  private int batchSize;
  private int maxRetries;
  private long retryBackoffMs;
  private long bufferLingerMs;
  private long readTimeoutMS;
  private long flushTimeoutMS;
  private int maxBufferedRecords;
  @Setter
  private volatile boolean stopRequested;
  @Setter
  private volatile boolean flushRequested;

  /**
   * Number of requests currently being sent into Elasticsearch
   */
  private AtomicInteger inFlightRecords;

  /**
   * Requests waiting to be sent into Elasticsearch
   */
  private ConcurrentLinkedQueue<IndexRequest> unsentRecords;

  public void init(){
    this.inFlightRecords = new AtomicInteger();
    this.unsentRecords = new ConcurrentLinkedQueue<>();
  }

  /**
   * Give the total number of requests in the connector task
   * @return the total number of requests in the connector task
   */
  public synchronized int bufferedRecords() {
    return unsentRecords.size() + inFlightRecords.get();
  }

  public void addRecord(IndexRequest request){
    this.unsentRecords.add(request);
  }

  private IndexRequest removeRecord(){
    return this.unsentRecords.poll();
  }

  private void incrementInFlightRecords(int batchSize){
    this.inFlightRecords.addAndGet(batchSize);
  }

  public void decrementInFlightRecords(int batchSize){
    this.inFlightRecords.addAndGet(- batchSize);
  }

  /**
   * Create a bulk of {@link IndexRequest}. Remove the firsts {@link ElasticsearchSinkConnectorConfig#BATCH_SIZE_CONFIG}
   * or the full queue, and increment the {@link SharedContext#inFlightRecords}
   *
   * @return the list or request the {@link BulkProcessorTask} will send to Elasticsearch
   */
  public synchronized List<IndexRequest> createBulk() {
    int bulkSize = Math.min(
        getBatchSize(), getUnsentRecords().size());
    List<IndexRequest> requests = new ArrayList<>(bulkSize);
    for (int i = 0; i < bulkSize; i++) {
      requests.add(removeRecord());
    }
    incrementInFlightRecords(bulkSize);
    return requests;
  }

}

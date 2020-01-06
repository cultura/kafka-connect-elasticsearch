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

package plugin.kafka.sink.elastic.workers;

import static plugin.kafka.sink.elastic.Orchestrator.BATCH_ID_GEN;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.ConnectException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig;
import plugin.kafka.sink.elastic.configs.SharedContext;
import plugin.kafka.sink.elastic.health.TaskHealth;

/**
 * This class is the worker of the connector. It's an infinite while loop who take a batch from
 * the {@link SharedContext#getUnsentRecords()} and send them to Elasticsearch.
 */
@Slf4j
public class BulkProcessorTask implements Runnable {

  private final long batchId = BATCH_ID_GEN.incrementAndGet();
  private SharedContext sharedContext;

  public BulkProcessorTask(SharedContext sharedContext) {
    this.sharedContext = sharedContext;
  }

  /**
   * The worker loop
   * A delay {@link BulkProcessorTask#waitBeforeCreatingBatch()} make sur the bulk is as close as possible
   * of {@link ElasticsearchSinkConnectorConfig#BATCH_SIZE_CONFIG} to use the Elasticsearch bulk API
   *
   * Unrecoverable exception are thrown as {@link ConnectException}, the thread factory handle it as
   * {@link TaskHealth#setException(ConnectException)}
   */
  @Override
  public void run() {
    log.info("---------------------------> farmer thread started");
    while (!sharedContext.isStopRequested()) {
      try {
        waitBeforeCreatingBatch();
        List<IndexRequest> bulk = sharedContext.createBulk();
        if (!bulk.isEmpty()){
          sendBulk(bulk);
          sharedContext.decrementInFlightRecords(bulk.size());
          log.debug("Successfully executed batch {} of {} records", batchId, bulk.size());
        }
      } catch (Exception e) {
        log.error("Unexpected error while waiting before flushing bulk", e);
        sharedContext.setStopRequested(true);
        throw new ConnectException(e);
      }
    }
  }

  /**
   * If there are less {@link IndexRequest} in the queue than {@link ElasticsearchSinkConnectorConfig#BATCH_SIZE_CONFIG},
   * wait {@link ElasticsearchSinkConnectorConfig#LINGER_MS_CONFIG} creating a bulk
   *
   * During flush process, delay is inactive because the queue has to be empty as fast as possible
   *
   * @throws InterruptedException
   */
  private void waitBeforeCreatingBatch() throws InterruptedException {
    if (!sharedContext.isFlushRequested() && sharedContext.getUnsentRecords().size() < sharedContext
        .getBatchSize()) {
      log.trace("{} ---------------------------> wait {} before creating batch, {}, {}", batchId, sharedContext
          .getLingerMs(), sharedContext.isFlushRequested(), sharedContext.getUnsentRecords().size());
      Thread.sleep(sharedContext.getLingerMs());
    }
  }

  /**
   * Create and send the bulk request to Elasticsearch. In case of failure, retry
   * {@link ElasticsearchSinkConnectorConfig#MAX_RETRIES_CONFIG} times and fail.
   *
   * @param bulk list of {@link IndexRequest} to send to Elasticsearch
   */
  private void sendBulk(List<IndexRequest> bulk) {
    BulkResponse bulkResponse;
    BulkRequest bulkRequest = new BulkRequest();
    bulk.forEach(bulkRequest::add);
    bulkRequest.timeout(new TimeValue(sharedContext.getReadTimeoutMS(), TimeUnit.MILLISECONDS));
    for (int i = 0; i <= sharedContext.getMaxRetries(); i++) {
      log.debug("batch {} ---------------------------> send bulk to els : {}", batchId,
          bulkRequest.numberOfActions());
      try {
        bulkResponse = sharedContext.getClient().bulk(bulkRequest);
        if (HandleResponse(bulk, bulkResponse)) {
          return;
        }
      } catch (IOException | RuntimeException e){
        log.error("batch {} ---------------------------> Unable to index data in during the {} attempt", batchId, i, e);
        this.sharedContext.getClient().resetClient();
      }
    }
    throw new ConnectException(
        "Unable to index data into elasticsearch after " + sharedContext.getMaxRetries() + " attempts");
  }

  /**
   * Check the response failures. Return true if no failures or data format exception : it does not depend on
   * Elasticsearch, format may be corrupted.
   *
   * Return false for every other failures.
   *
   * @param bulk use for logging purpose
   * @param bulkResponse the response with possible failure
   * @return true or false depending of failure message
   */
  private boolean HandleResponse(List<IndexRequest> bulk, BulkResponse bulkResponse) {
    if (!bulkResponse.hasFailures()) {
      log.debug("batch {} ---------------------------> successfully send {} records into elastic",
          batchId, bulk.size());
      return true;
    } else {
      String failureMessage = bulkResponse.buildFailureMessage();
      log.error("---------------------------> Unable to index data : {}", failureMessage);
      if (failureMessage.contains("mapper_parsing_exception")
          || failureMessage.contains("illegal_argument_exception")) {
        return true;
      }
    }
    try {
      Thread.sleep(sharedContext.getRetryBackoffMs());
    } catch (InterruptedException e) {
      throw new ConnectException(e);
    }
    return false;
  }
}

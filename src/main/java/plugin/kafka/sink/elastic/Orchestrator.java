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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.errors.ConnectException;
import org.elasticsearch.action.index.IndexRequest;
import plugin.kafka.sink.elastic.client.ElasticClient;
import plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig;
import plugin.kafka.sink.elastic.configs.SharedContext;
import plugin.kafka.sink.elastic.health.TaskHealth;
import plugin.kafka.sink.elastic.queue.QueueDelayer;
import plugin.kafka.sink.elastic.workers.BulkProcessorTask;

/**
 * This class is the brain of the connector. It handle the thread pool, put the records into the
 * {@link ConcurrentLinkedQueue}, handle the flush and close connections.
 */
@Builder
@Slf4j
public class Orchestrator {

  public static final AtomicLong BATCH_ID_GEN = new AtomicLong();
  private QueueDelayer queueDelayer;
  private ThreadPoolExecutor executor;
  @Getter
  private SharedContext sharedContext;

  /**
   * Init the {@link QueueDelayer} and the thread pool with the instances of {@link BulkProcessorTask}
   */
  public void initOrchestrator(){
    this.queueDelayer = QueueDelayer.builder()
        .time(new SystemTime())
        .sharedContext(sharedContext)
        .build();
    ThreadFactory threadFactory = makeThreadFactory();
    this.executor = (ThreadPoolExecutor) Executors
        .newFixedThreadPool(sharedContext.getMaxInFlightRequests(), threadFactory);
    for (int i = 0; i < sharedContext.getMaxInFlightRequests(); i++) {
      executor.execute(new BulkProcessorTask(sharedContext));
    }
  }

  /**
   * Create a thread factory with exception hook to stop the connector task if an exception is thrown
   * in a {@link BulkProcessorTask}
   *
   * @return the thread factory with the proper configuration
   */
  private ThreadFactory makeThreadFactory() {
    final AtomicInteger threadCounter = new AtomicInteger();
    final Thread.UncaughtExceptionHandler uncaughtExceptionHandler =
        (t, e) -> {
          log.error("Uncaught exception in BulkProcessor thread {}", t, e);
          failAndStop(e);
          throw sharedContext.getTaskHealth().getException();
        };
    return new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        final int threadId = threadCounter.getAndIncrement();
        final int objId = System.identityHashCode(this);
        final Thread t = new Thread(r, String.format("BulkProcessor@%d-%d", objId, threadId));
        t.setDaemon(true);
        t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return t;
      }
    };
  }

  /**
   * Add request into the {@link SharedContext#getUnsentRecords()} if the buffer is not full
   * ({@link ElasticsearchSinkConnectorConfig#MAX_BUFFERED_RECORDS_CONFIG}).
   * Else, wait {@link ElasticsearchSinkConnectorConfig#BUFFERED_LINGER_MS_CONFIG}.
   * If the buffer is still full, throw a {@link ConnectException}
   *
   * if Task is in failure state ({@link TaskHealth}), throw a {@link ConnectException}
   *
   * @param request the index request
   */
  public void addRequest(IndexRequest request) {
    sharedContext.getTaskHealth().throwIfException();
    if (sharedContext.bufferedRecords() >= sharedContext.getMaxBufferedRecords()) {
      queueDelayer.waitBeforeAddRequest();
      sharedContext.getTaskHealth().throwIfException();
      if (sharedContext.bufferedRecords() >= sharedContext.getMaxBufferedRecords()) {
        throw new ConnectException("Add timeout expired before buffer availability");
      }
    }
    sharedContext.addRecord(request);
  }

  /**
   * Wait until the buffer is empty or timeout is reach ({@link ElasticsearchSinkConnectorConfig#FLUSH_TIMEOUT_MS_CONFIG})
   *
   * if Task is in failure state ({@link TaskHealth}), throw a {@link ConnectException}
   */
  public void flush() {
    sharedContext.getTaskHealth().throwIfException();
    log.info("---------------------------> start flush with unsent records : {}",
        sharedContext.bufferedRecords());
    sharedContext.setFlushRequested(true);
    try {
      queueDelayer.waitDuringFlush();
      sharedContext.getTaskHealth().throwIfException();
      if (sharedContext.bufferedRecords() > 0) {
        throw new ConnectException(
            "Flush timeout expired with unflushed records: " + sharedContext.bufferedRecords());
      }
    } catch (InterruptedException e) {
      executor.purge();
      throw new ConnectException(e);
    } finally {
      sharedContext.setFlushRequested(false);
    }
    log.info("---------------------------> flush terminated with success");
  }

  /**
   * Set the task state to failure and {@link Orchestrator#stop()} the task
   * @param e the uncaught exception
   */
  private void failAndStop(Throwable e) {
    sharedContext.getTaskHealth().setException(new ConnectException(e));
    stop();
  }

  /**
   * Empty and close the {@link ThreadPoolExecutor}, and close the {@link ElasticClient}
   */
  public void stop() {
    this.sharedContext.setStopRequested(true);
    stopExecutor();
    this.sharedContext.getClient().close();
  }

  /**
   * Close the {@link ThreadPoolExecutor} properly if possible, force close it else
   */
  private void stopExecutor() {
    synchronized (this) {
      executor.shutdown();
    }
    try {
      if (!executor.awaitTermination(sharedContext.getFlushTimeoutMS(), TimeUnit.MILLISECONDS)) {
        throw new ConnectException("Timed-out waiting for executor termination");
      }
    } catch (InterruptedException e) {
      throw new ConnectException(e);
    } finally {
      executor.shutdownNow();
    }
  }

}

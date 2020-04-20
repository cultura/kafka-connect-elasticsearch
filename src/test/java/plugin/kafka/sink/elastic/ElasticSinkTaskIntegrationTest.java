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

package plugin.kafka.sink.elastic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG;
import static plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig.BUFFERED_LINGER_MS_CONFIG;
import static plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG;
import static plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG;
import static plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig.INDEX_NAME;
import static plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG;
import static plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG;
import static plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG;
import static plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG;
import static plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig.NUMBER_OF_REPLICA;
import static plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig.NUMBER_OF_SHARD;
import static plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG;
import static plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.awaitility.Awaitility;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import plugin.kafka.sink.elastic.client.ElasticClient;
import plugin.kafka.sink.elastic.models.SinkRecordsHandler;

public class ElasticSinkTaskIntegrationTest {

  private static final long BUFFERED_LINGER_MS_VALUE = 300L;
  private static final int MAX_RETRY_VALUE = 2;
  private static final long LINGER_MS_VALUE = 150L;
  private static final long FLUSH_TIMEOUT_VALUE = 400L;
  private static final int BATCH_SIZE_VALUE = 2;
  private static final int MAX_BUFFERED_RECORDS_VALUE = 4;
  private static final String INDEX_TEST_NAME = "index_test";

  private BulkResponse bulkResponse = mock(BulkResponse.class);
  private ElasticClient client = mock(ElasticClient.class);

  private ElasticSinkTask task = new ElasticSinkTask();

  private SinkRecordsHandler sinkRecordsHandler = new SinkRecordsHandler();
  private List<DocWriteRequest> requests = new ArrayList<>();
  private Time time = new SystemTime();

  @BeforeEach
  public void setUp() {
    HashMap<String, String> props = new HashMap<>();

    props.put(FLUSH_TIMEOUT_MS_CONFIG, String.valueOf(FLUSH_TIMEOUT_VALUE));
    props.put(READ_TIMEOUT_MS_CONFIG, "60");
    props.put(INDEX_TEST_NAME, INDEX_TEST_NAME);
    props.put(NUMBER_OF_SHARD, "5");
    props.put(NUMBER_OF_REPLICA, "0");
    props.put(MAX_BUFFERED_RECORDS_CONFIG, String.valueOf(MAX_BUFFERED_RECORDS_VALUE));
    props.put(BUFFERED_LINGER_MS_CONFIG, String.valueOf(BUFFERED_LINGER_MS_VALUE));
    props.put(BATCH_SIZE_CONFIG, String.valueOf(BATCH_SIZE_VALUE));
    props.put(LINGER_MS_CONFIG, String.valueOf(LINGER_MS_VALUE));
    props.put(MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
    props.put(RETRY_BACKOFF_MS_CONFIG, "1");
    props.put(MAX_RETRIES_CONFIG, String.valueOf(MAX_RETRY_VALUE));
    props.put(CONNECTION_URL_CONFIG, "unused");
    props.put(INDEX_NAME, INDEX_TEST_NAME);
    task.start(props);
    this.task.setClient(this.client);
    this.task.getOrchestrator().getSharedContext().setClient(this.client);
  }

  @AfterEach
  public void cleanUp(){
    this.task.stop();
    this.sinkRecordsHandler.clearRecords();
    this.requests.clear();
  }

  @Test
  public void should_index_one_message() throws IOException {
    mockClientWithRequestsRetrieve();

    sinkRecordsHandler.addRecord();

    task.put(sinkRecordsHandler.getRecords());
    Awaitility.await()
        .atMost(LINGER_MS_VALUE, TimeUnit.MILLISECONDS).until(() -> !requests.isEmpty());
    assertEquals(1, requests.size());
    DocWriteRequest request = requests.get(0);
    assertEquals("key", request.id());
    assertEquals(INDEX_TEST_NAME, request.index());
    assertThat(request.toString(), containsString("{\"user\":\"john\",\"message\":\"test elastic 7\"}"));
  }

  @Test
  public void should_index_in_two_bulk() throws IOException {
    mockClientWithRequestsRetrieve();

    sinkRecordsHandler.generateRandomMessages(BATCH_SIZE_VALUE + 1);
    long start = time.milliseconds();
    task.put(sinkRecordsHandler.getRecords());
    Awaitility.await()
        .atMost(3*LINGER_MS_VALUE, TimeUnit.MILLISECONDS).until(() -> requests.size() == 3);
    assertTrue(time.milliseconds() - start >= LINGER_MS_VALUE);
    verify(this.client, times(2)).bulk(any());
  }

  @Test
  public void should_wait_before_adding_request_with_full_buffer() throws IOException {
    mockClientWithRequestsRetrieve();

    sinkRecordsHandler.generateRandomMessages(MAX_BUFFERED_RECORDS_VALUE + 1);
    long start = time.milliseconds();
    task.put(sinkRecordsHandler.getRecords());
    long stop = time.milliseconds();
    Awaitility.await().atMost(LINGER_MS_VALUE, TimeUnit.MILLISECONDS).until(() -> requests.size() == MAX_BUFFERED_RECORDS_VALUE + 1);
    assertTrue(stop-start >= BUFFERED_LINGER_MS_VALUE);
  }

  @Test
  public void should_index_without_retry_with_specific_indexation_error_message() throws IOException {
    mockClientWithRequestsRetrieve();
    doReturn(true).when(this.bulkResponse).hasFailures();
    when(this.bulkResponse.buildFailureMessage())
        .thenReturn("mapper_parsing_exception")
        .thenReturn("illegal_argument_exception");

    sinkRecordsHandler.generateRandomMessages(1);
    task.put(sinkRecordsHandler.getRecords());
    Awaitility.await().atMost(LINGER_MS_VALUE, TimeUnit.MILLISECONDS).until(() -> requests.size() == 1);
    task.put(sinkRecordsHandler.getRecords());
    Awaitility.await().atMost(LINGER_MS_VALUE, TimeUnit.MILLISECONDS).until(() -> requests.size() == 2);

    assertEquals(2, requests.size());
  }

  @Test
  public void should_retry_and_fail_with_indexation_error() throws IOException {
    mockClientWithRequestsRetrieve();
    doReturn(true).when(this.bulkResponse).hasFailures();
    when(this.bulkResponse.buildFailureMessage()).thenReturn("test");

    sinkRecordsHandler.generateRandomMessages(1);
    task.put(sinkRecordsHandler.getRecords());
    Awaitility.await().atMost(LINGER_MS_VALUE, TimeUnit.MILLISECONDS).until(() -> requests.size() == MAX_RETRY_VALUE + 1);
    assertThrows(ConnectException.class, () -> task.put(sinkRecordsHandler.getRecords()));
  }

  @Test
  public void should_timeout_with_buffer_full() throws IOException {
    doAnswer(invocation -> {
      Thread.sleep(FLUSH_TIMEOUT_VALUE + BUFFERED_LINGER_MS_VALUE);
      requests.addAll(((BulkRequest) invocation.getArguments()[0]).requests());
      return bulkResponse;
    }).when(this.client).bulk(any());

    sinkRecordsHandler.generateRandomMessages(MAX_BUFFERED_RECORDS_VALUE + 1);
    assertThrows(ConnectException.class, () -> task.put(sinkRecordsHandler.getRecords()));

  }

  @Test
  public void should_reset_client_after_error() throws IOException {
    when(this.client.bulk(any())).thenThrow(new RuntimeException("test")).thenAnswer(invocation -> {
      requests.addAll(((BulkRequest) invocation.getArguments()[0]).requests());
      return bulkResponse;
    });

    sinkRecordsHandler.generateRandomMessages(1);
    task.put(sinkRecordsHandler.getRecords());
    Awaitility.await().atMost(LINGER_MS_VALUE, TimeUnit.MILLISECONDS).until(() -> requests.size() == 1);

    verify(this.client, times(1)).resetClient();

  }

  @Test
  public void should_reset_client_only_once_after_multiple_errors() throws IOException {
    doCallRealMethod().when(this.client).setLastRestart(anyLong());
    this.client.setLastRestart(0L);
    AtomicInteger callIteration = new AtomicInteger();
    doAnswer(invocation -> {
      int iteration = callIteration.incrementAndGet();
      if (iteration < MAX_RETRY_VALUE + 1){
        throw new RuntimeException("test");
      } else {
        return bulkResponse;
      }
    }).when(this.client).bulk(any());

    doCallRealMethod().when(this.client).resetClient();

    sinkRecordsHandler.generateRandomMessages(1);
    task.put(sinkRecordsHandler.getRecords());
    Awaitility.await().atMost(LINGER_MS_VALUE, TimeUnit.MILLISECONDS).until(() -> callIteration.get() == MAX_RETRY_VALUE + 1);

    verify(this.client, times(MAX_RETRY_VALUE)).resetClient();
    verify(this.client, times(1)).close();
  }

  @Test
  public void should_throw_exception_after_all_error_retry_fail() throws IOException {
    AtomicInteger callIteration = new AtomicInteger();
    doAnswer(invocation -> {
      callIteration.incrementAndGet();
      throw new RuntimeException("test");
    }).when(this.client).bulk(any());

    sinkRecordsHandler.generateRandomMessages(1);
    task.put(sinkRecordsHandler.getRecords());
    Awaitility.await().atMost(LINGER_MS_VALUE, TimeUnit.MILLISECONDS).until(() -> callIteration.get() == MAX_RETRY_VALUE + 1);

    assertThrows(ConnectException.class, () -> task.put(sinkRecordsHandler.getRecords()));
  }

  @Test
  public void should_throw_exception_with_buffer_full_in_error_state() throws IOException {
    doThrow(new RuntimeException("test")).when(this.client).bulk(any());
    sinkRecordsHandler.generateRandomMessages(MAX_BUFFERED_RECORDS_VALUE + 1);
    assertThrows(ConnectException.class, () -> task.put(sinkRecordsHandler.getRecords()));
  }

  @Test
  public void should_flush_fast_when_buffer_empty() {
    long start = time.milliseconds();
    task.flush(null);
    assertTrue(time.milliseconds() - start < LINGER_MS_VALUE);
  }

  @Test
  public void should_flush_without_waiting_if_flush_with_full_buffer() throws IOException {
    mockClientWithRequestsRetrieve();
    sinkRecordsHandler.generateRandomMessages(MAX_BUFFERED_RECORDS_VALUE);
    task.put(sinkRecordsHandler.getRecords());
    task.flush(null);
    Awaitility.await().atMost(LINGER_MS_VALUE, TimeUnit.MILLISECONDS).until(() -> requests.size() == MAX_BUFFERED_RECORDS_VALUE);
  }

  @Test
  public void should_flush_throw_exception_after_timeout() throws IOException {
    doAnswer(invocation -> {
      Thread.sleep(FLUSH_TIMEOUT_VALUE + LINGER_MS_VALUE);
      requests.addAll(((BulkRequest) invocation.getArguments()[0]).requests());
      return bulkResponse;
    }).when(this.client).bulk(any());
    sinkRecordsHandler.generateRandomMessages(1);
    task.put(sinkRecordsHandler.getRecords());
    assertThrows(ConnectException.class, () -> task.flush(null));
  }

  @Test
  public void should_flush_throw_exception_when_start_in_error_state() throws IOException {
    doThrow(new RuntimeException("test")).when(this.client).bulk(any());
    sinkRecordsHandler.generateRandomMessages(1);
    task.put(sinkRecordsHandler.getRecords());
    assertThrows(ConnectException.class, () -> task.flush(null));
  }

  public void mockClientWithRequestsRetrieve() throws IOException {
    doAnswer(invocation -> {
      requests.addAll(((BulkRequest) invocation.getArguments()[0]).requests());
      return bulkResponse;
    }).when(this.client).bulk(any());
  }
}

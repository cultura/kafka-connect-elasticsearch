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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.connect.errors.ConnectException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import plugin.kafka.sink.elastic.configs.ElasticConfig;
import plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig;

/**
 * This class is here only for test purpose. It allow us to mock client creation.
 */
@Slf4j
public final class ClientBuilder {

  private ClientBuilder() {
  }

  public static RestHighLevelClient initClient(ElasticConfig elasticConfig,
      ElasticsearchSinkConnectorConfig config) {
    return new RestHighLevelClient(RestClient.builder(elasticConfig.getHostUrls().stream()
        .map(ElasticClient::buildElasticHost)
        .toArray(HttpHost[]::new))
        .setRequestConfigCallback(
            requestConfigBuilder -> requestConfigBuilder
                .setConnectTimeout(0)
                .setSocketTimeout(elasticConfig.getReadTimeout()))
        .setHttpClientConfigCallback(
            (HttpAsyncClientBuilder httpClientBuilder) -> buildSecuredClient(elasticConfig, config,
                httpClientBuilder)));
  }

  public static HttpAsyncClientBuilder buildSecuredClient(ElasticConfig elasticConfig,
      ElasticsearchSinkConnectorConfig config, HttpAsyncClientBuilder httpClientBuilder) {
    if (isNotBlank(elasticConfig.getUserName()) && isNotBlank(elasticConfig.getPassword())) {
      httpClientBuilder.setDefaultCredentialsProvider(buildBasicAuth(elasticConfig));
    }
    if (config.secured()) {
      httpClientBuilder.setSSLContext(buildSslContext(config));
    }
    return httpClientBuilder;
  }

  public static SSLContext buildSslContext(ElasticsearchSinkConnectorConfig config) {
    try (InputStream is = Files
        .newInputStream(Paths.get(config.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG)))) {
      KeyStore keyStore = KeyStore
          .getInstance(config.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG));
      keyStore.load(is, config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).value().toCharArray());
      SSLContextBuilder sslBuilder = SSLContexts.custom().loadTrustMaterial(keyStore, null);
      return sslBuilder.build();
    } catch (Exception e) {
      log.error("---------------------------> Unable to build ssl context for secured els client",
          e);
      throw new ConnectException("Unable to build ssl context", e);
    }
  }

  public static BasicCredentialsProvider buildBasicAuth(ElasticConfig elasticConfig) {
    BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(elasticConfig.getUserName(),
            elasticConfig.getPassword()));
    return credentialsProvider;
  }

}

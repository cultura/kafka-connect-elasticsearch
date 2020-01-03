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

package plugin.kafka.sink.elastic.utils;

import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.BINARY_TYPE;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.BOOLEAN_TYPE;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.BYTE_TYPE;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.DOUBLE_TYPE;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.FLOAT_TYPE;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.INTEGER_TYPE;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.KEYWORD_TYPE;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.LONG_TYPE;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.MAP_KEY;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.MAP_VALUE;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.SHORT_TYPE;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.TEXT_TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants;

public final class Mapping {

  private Mapping() {}

  /**
   * Infer mapping from the provided schema.
   *
   * @param schema The schema used to infer mapping.
   */
  public static JsonNode inferMapping(Schema schema) {
    if (schema == null) {
      throw new DataException("Cannot infer mapping without schema.");
    }

    // Handle logical types
    JsonNode logicalConversion = inferLogicalMapping(schema);
    if (logicalConversion != null) {
      return logicalConversion;
    }

    Schema.Type schemaType = schema.type();
    ObjectNode properties = JsonNodeFactory.instance.objectNode();
    ObjectNode fields = JsonNodeFactory.instance.objectNode();
    switch (schemaType) {
      case ARRAY:
        return inferMapping(schema.valueSchema());
      case MAP:
        properties.set("properties", fields);
        fields.set(MAP_KEY, inferMapping(schema.keySchema()));
        fields.set(MAP_VALUE, inferMapping(schema.valueSchema()));
        return properties;
      case STRUCT:
        properties.set("properties", fields);
        for (Field field : schema.fields()) {
          fields.set(field.name(), inferMapping(field.schema()));
        }
        return properties;
      default:
        String esType = getElasticsearchType(schemaType);
        return inferPrimitive(esType, schema.defaultValue());
    }
  }

  // visible for testing
  protected static String getElasticsearchType(Schema.Type schemaType) {
    switch (schemaType) {
      case BOOLEAN:
        return BOOLEAN_TYPE;
      case INT8:
        return BYTE_TYPE;
      case INT16:
        return SHORT_TYPE;
      case INT32:
        return INTEGER_TYPE;
      case INT64:
        return LONG_TYPE;
      case FLOAT32:
        return FLOAT_TYPE;
      case FLOAT64:
        return DOUBLE_TYPE;
      case STRING:
        return TEXT_TYPE;
      case BYTES:
        return BINARY_TYPE;
      default:
        return null;
    }
  }

  private static JsonNode inferLogicalMapping(Schema schema) {
    String schemaName = schema.name();
    Object defaultValue = schema.defaultValue();
    if (schemaName == null) {
      return null;
    }

    switch (schemaName) {
      case Date.LOGICAL_NAME:
      case Time.LOGICAL_NAME:
      case Timestamp.LOGICAL_NAME:
        return inferPrimitive(ElasticsearchSinkConnectorConstants.DATE_TYPE, defaultValue);
      case Decimal.LOGICAL_NAME:
        return inferPrimitive(ElasticsearchSinkConnectorConstants.DOUBLE_TYPE, defaultValue);
      default:
        // User-defined type or unknown built-in
        return null;
    }
  }

  private static JsonNode inferPrimitive(String type, Object defaultValue) {
    if (type == null) {
      throw new ConnectException("Invalid primitive type.");
    }

    ObjectNode obj = JsonNodeFactory.instance.objectNode();
    obj.set("type", JsonNodeFactory.instance.textNode(type));
    if (type.equals(TEXT_TYPE)) {
      addTextMapping(obj);
    }
    JsonNode defaultValueNode = null;
    if (defaultValue != null) {
      switch (type) {
        case ElasticsearchSinkConnectorConstants.BYTE_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((byte) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.SHORT_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((short) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.INTEGER_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((int) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.LONG_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((long) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.FLOAT_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((float) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.DOUBLE_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((double) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.STRING_TYPE:
        case ElasticsearchSinkConnectorConstants.TEXT_TYPE:
        case ElasticsearchSinkConnectorConstants.BINARY_TYPE:
          // IGNORE default values for text and binary types as this is not supported by ES side.
          // see https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html
          // https://www.elastic.co/guide/en/elasticsearch/reference/current/binary.html
          // for more details.
          //defaultValueNode = null;
          break;
        case ElasticsearchSinkConnectorConstants.BOOLEAN_TYPE:
          defaultValueNode = JsonNodeFactory.instance.booleanNode((boolean) defaultValue);
          break;
        case ElasticsearchSinkConnectorConstants.DATE_TYPE:
          defaultValueNode = JsonNodeFactory.instance.numberNode((long) defaultValue);
          break;
        default:
          throw new DataException("Invalid primitive type.");
      }
    }
    if (defaultValueNode != null) {
      obj.set("null_value", defaultValueNode);
    }
    return obj;
  }

  private static void addTextMapping(ObjectNode obj) {
    // Add additional mapping for indexing, per https://www.elastic.co/blog/strings-are-dead-long-live-strings
    ObjectNode keyword = JsonNodeFactory.instance.objectNode();
    keyword.set("type", JsonNodeFactory.instance.textNode(KEYWORD_TYPE));
    keyword.set("ignore_above", JsonNodeFactory.instance.numberNode(256));
    ObjectNode fields = JsonNodeFactory.instance.objectNode();
    fields.set("keyword", keyword);
    obj.set("fields", fields);
  }

  private static byte[] bytes(Object value) {
    final byte[] bytes;
    if (value instanceof ByteBuffer) {
      final ByteBuffer buffer = ((ByteBuffer) value).slice();
      bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
    } else {
      bytes = (byte[]) value;
    }
    return bytes;
  }

}

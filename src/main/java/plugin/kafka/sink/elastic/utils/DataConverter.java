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

package plugin.kafka.sink.elastic.utils;

import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.MAP_KEY;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.MAP_VALUE;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

@Slf4j
public class DataConverter {

  private static final Converter JSON_CONVERTER;

  static {
    JSON_CONVERTER = new JsonConverter();
    JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
  }

  public static String convertKey(SinkRecord record) {
    Schema keySchema = record.keySchema();
    Object key = record.key();
    if (key == null) {
      return null;
    }

    final Schema.Type schemaType;
    if (keySchema == null) {
      schemaType = ConnectSchema.schemaType(key.getClass());
      if (schemaType == null) {
        throw new DataException(
            "Java class "
                + key.getClass()
                + " does not have corresponding schema type."
        );
      }
    } else {
      schemaType = keySchema.type();
    }

    switch (schemaType) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case STRING:
        return String.valueOf(key);
      default:
        throw new DataException(schemaType.name() + " is not supported as the document id.");
    }
  }

  public static String getPayload(SinkRecord record){
    if (record.value() == null) {
      return null;
    }

    Schema schema = preProcessSchema(record.valueSchema());

    Object value = preProcessValue(record.value(), record.valueSchema(), schema);

    byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(record.topic(), schema, value);
    return new String(rawJsonPayload, StandardCharsets.UTF_8);
  }

  protected static Schema preProcessSchema(Schema schema) {
    if (schema == null) {
      return null;
    }
    // Handle logical types
    String schemaName = schema.name();
    if (schemaName != null) {
      switch (schemaName) {
        case Decimal.LOGICAL_NAME:
          return copySchemaBasics(schema, SchemaBuilder.float64()).build();
        case Date.LOGICAL_NAME:
        case Time.LOGICAL_NAME:
        case Timestamp.LOGICAL_NAME:
          return schema;
        default:
          // User type or unknown logical type
          break;
      }
    }

    Schema.Type schemaType = schema.type();
    switch (schemaType) {
      case ARRAY:
        return preProcessArraySchema(schema);
      case MAP:
        return preProcessMapSchema(schema);
      case STRUCT:
        return preProcessStructSchema(schema);
      default:
        return schema;
    }
  }

  private static SchemaBuilder copySchemaBasics(Schema source, SchemaBuilder target) {
    if (source.isOptional()) {
      target.optional();
    }
    if (source.defaultValue() != null && source.type() != Schema.Type.STRUCT) {
      final Object defaultVal = preProcessValue(source.defaultValue(), source, target);
      target.defaultValue(defaultVal);
    }
    return target;
  }

  protected static Object preProcessValue(Object value, Schema schema, Schema newSchema) {
    // Handle missing schemas and acceptable null values
    if (schema == null) {
      return value;
    }

    if (value == null) {
      return preProcessNullValue(schema);
    }

    // Handle logical types
    String schemaName = schema.name();
    if (schemaName != null) {
      Object result = preProcessLogicalValue(schemaName, value);
      if (result != null) {
        return result;
      }
    }

    Schema.Type schemaType = schema.type();
    switch (schemaType) {
      case ARRAY:
        return preProcessArrayValue(value, schema, newSchema);
      case MAP:
        return preProcessMapValue(value, schema, newSchema);
      case STRUCT:
        return preProcessStructValue(value, schema, newSchema);
      default:
        return value;
    }
  }

  private static Object preProcessLogicalValue(String schemaName, Object value) {
    switch (schemaName) {
      case Decimal.LOGICAL_NAME:
        return ((BigDecimal) value).doubleValue();
      case Date.LOGICAL_NAME:
      case Time.LOGICAL_NAME:
      case Timestamp.LOGICAL_NAME:
        return value;
      default:
        // User-defined type or unknown built-in
        return null;
    }
  }

  private static Object preProcessNullValue(Schema schema) {
    if (schema.defaultValue() != null) {
      return schema.defaultValue();
    }
    if (schema.isOptional()) {
      return null;
    }
    throw new DataException("null value for field that is required and has no default value");
  }

  private static Object preProcessArrayValue(Object value, Schema schema, Schema newSchema) {
    Collection<?> collection = (Collection<?>) value;
    List<Object> result = new ArrayList<>();
    for (Object element: collection) {
      result.add(preProcessValue(element, schema.valueSchema(), newSchema.valueSchema()));
    }
    return result;
  }

  private static Object preProcessMapValue(Object value, Schema schema, Schema newSchema) {
    Schema keySchema = schema.keySchema();
    Schema valueSchema = schema.valueSchema();
    Schema newValueSchema = newSchema.valueSchema();
    Map<?, ?> map = (Map<?, ?>) value;
    if (keySchema.type() == Schema.Type.STRING) {
      Map<Object, Object> processedMap = new HashMap<>();
      for (Map.Entry<?, ?> entry: map.entrySet()) {
        processedMap.put(
            preProcessValue(entry.getKey(), keySchema, newSchema.keySchema()),
            preProcessValue(entry.getValue(), valueSchema, newValueSchema)
        );
      }
      return processedMap;
    }
    List<Struct> mapStructs = new ArrayList<>();
    for (Map.Entry<?, ?> entry: map.entrySet()) {
      Struct mapStruct = new Struct(newValueSchema);
      Schema mapKeySchema = newValueSchema.field(MAP_KEY).schema();
      Schema mapValueSchema = newValueSchema.field(MAP_VALUE).schema();
      mapStruct.put(MAP_KEY, preProcessValue(entry.getKey(), keySchema, mapKeySchema));
      mapStruct.put(MAP_VALUE, preProcessValue(entry.getValue(), valueSchema, mapValueSchema));
      mapStructs.add(mapStruct);
    }
    return mapStructs;
  }

  private static Object preProcessStructValue(Object value, Schema schema, Schema newSchema) {
    Struct struct = (Struct) value;
    Struct newStruct = new Struct(newSchema);
    for (Field field : schema.fields()) {
      Schema newFieldSchema = newSchema.field(field.name()).schema();
      Object converted = preProcessValue(struct.get(field), field.schema(), newFieldSchema);
      newStruct.put(field.name(), converted);
    }
    return newStruct;
  }

  private static Schema preProcessArraySchema(Schema schema) {
    Schema valSchema = preProcessSchema(schema.valueSchema());
    return copySchemaBasics(schema, SchemaBuilder.array(valSchema)).build();
  }

  private static Schema preProcessMapSchema(Schema schema) {
    Schema keySchema = schema.keySchema();
    Schema valueSchema = schema.valueSchema();
    String keyName = keySchema.name() == null ? keySchema.type().name() : keySchema.name();
    String valueName = valueSchema.name() == null ? valueSchema.type().name() : valueSchema.name();
    Schema preprocessedKeySchema = preProcessSchema(keySchema);
    Schema preprocessedValueSchema = preProcessSchema(valueSchema);
    if (keySchema.type() == Schema.Type.STRING) {
      SchemaBuilder result = SchemaBuilder.map(preprocessedKeySchema, preprocessedValueSchema);
      return copySchemaBasics(schema, result).build();
    }
    Schema elementSchema = SchemaBuilder.struct().name(keyName + "-" + valueName)
        .field(MAP_KEY, preprocessedKeySchema)
        .field(MAP_VALUE, preprocessedValueSchema)
        .build();
    return copySchemaBasics(schema, SchemaBuilder.array(elementSchema)).build();
  }

  private static Schema preProcessStructSchema(Schema schema) {
    SchemaBuilder builder = copySchemaBasics(schema, SchemaBuilder.struct().name(schema.name()));
    for (Field field : schema.fields()) {
      builder.field(field.name(), preProcessSchema(field.schema()));
    }
    return builder.build();
  }

}

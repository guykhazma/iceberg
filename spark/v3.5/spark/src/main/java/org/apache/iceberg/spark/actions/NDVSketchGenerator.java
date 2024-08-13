/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.actions;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.stats.ThetaSketchAggregator;

public class NDVSketchGenerator {

  private NDVSketchGenerator() {}

  public static final String APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY = "ndv";

  static List<Blob> generateNDVSketchesAndBlobs(
      SparkSession spark, Table table, Snapshot snapshot, Set<String> columns) {
    Map<Integer, Object> minValues = Maps.newHashMap();
    Map<Integer, Object> maxValues = Maps.newHashMap();
    Map<Integer, Sketch> columnToSketchMap = computeNDVSketches(spark, table, snapshot,
            columns, minValues, maxValues);
    return generateBlobs(table, columns, columnToSketchMap,
            minValues,
            maxValues,
            snapshot);
  }

  private static List<Blob> generateBlobs(
      Table table, Set<String> columns,
      Map<Integer, Sketch> sketchMap,
      Map<Integer, Object> minValues,
      Map<Integer, Object> maxValues,
      Snapshot snapshot) {
    return columns.stream()
        .map(
            columnName -> {
              Schema schema = table.schemas().get(snapshot.schemaId());
              Types.NestedField field = schema.findField(columnName);
              Sketch sketch = sketchMap.get(field.fieldId());
              long ndv = (long) sketch.getEstimate();
              return new Blob(
                  StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                  ImmutableList.of(field.fieldId()),
                  snapshot.snapshotId(),
                  snapshot.sequenceNumber(),
                  ByteBuffer.wrap(sketch.toByteArray()),
                  null,
                  ImmutableMap.of(
                          APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY, String.valueOf(ndv),
                          "min", String.valueOf(minValues.get(field.fieldId())),
                          "max", String.valueOf(maxValues.get(field.fieldId()))
                  ));
            })
        .collect(Collectors.toList());
  }

  private static Map<Integer, Sketch> computeNDVSketches(
      SparkSession spark, Table table, Snapshot snapshot, Set<String> columnsToBeAnalyzed,
      Map<Integer, Object> minValues, Map<Integer, Object> maxValues) {
    Map<Integer, Sketch> sketchMap = Maps.newHashMap();
    String tableName = table.name();
    List<String> columns = ImmutableList.copyOf(columnsToBeAnalyzed);

    Column[] thetaSketchColumns =
        columns.stream()
            .map(
                columnName -> {
                  ThetaSketchAggregator thetaSketchAggregator =
                      new ThetaSketchAggregator(new Column(columnName).expr());
                  return new Column(thetaSketchAggregator.toAggregateExpression());
                })
            .toArray(Column[]::new);
    Column[] minColumns =
          columns.stream()
                  .map(columnName -> new Column(functions.min(columnName).expr()))
                  .toArray(Column[]::new);
    Column[] maxColumns =
          columns.stream()
                  .map(columnName -> new Column(functions.max(columnName).expr()))
                  .toArray(Column[]::new);
    Dataset<Row> sketches =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SNAPSHOT_ID, snapshot.snapshotId())
            .load(tableName)
            .select(ArrayUtil.concat(Column.class,
                    thetaSketchColumns, minColumns, maxColumns));

    Row rows = sketches.collectAsList().get(0);
    Schema schema = table.schemas().get(snapshot.schemaId());
    for (int i = 0; i < columns.size(); i++) {
      Types.NestedField field = schema.findField(columns.get(i));
      sketchMap.put(field.fieldId(), Sketches.wrapSketch(Memory.wrap((byte[]) rows.get(i))));
      // TODO: shouldn't be done this way
      minValues.put(field.fieldId(), rows.get(i + columns.size()));
      maxValues.put(field.fieldId(), rows.get(i + columns.size()));
    }
    return sketchMap;
  }
}

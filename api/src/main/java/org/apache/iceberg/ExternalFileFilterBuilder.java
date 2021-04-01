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

package org.apache.iceberg;

import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;

public class ExternalFileFilterBuilder implements FileFilterBuilder {
  private String fileFilterImpl;
  private Table table;

  /*
  Load implementation by reflection
  */
  private static Evaluator forTable(String className, Table table, Expression fileFilter) {
    try {
      DynConstructors.Ctor<Evaluator> implConstructor =
              DynConstructors.builder().hiddenImpl(className, Table.class, Expression.class).buildChecked();
      return implConstructor.newInstance(table, fileFilter);
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  public ExternalFileFilterBuilder fileFilterImpl(String filterImpl) {
    this.fileFilterImpl = filterImpl;
    return this;
  }

  public ExternalFileFilterBuilder table(Table tbl) {
    this.table = tbl;
    return this;
  }

  @Override
  public Evaluator build(Expression fileFilter) {
    return forTable(fileFilterImpl, table, fileFilter);
  }
}

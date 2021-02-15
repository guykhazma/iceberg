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

package org.apache.iceberg.expressions;

import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.common.DynConstructors;

public interface EvaluatorInterface {
  /*
  Load implementation by reflection
  */
  static EvaluatorInterface forTable(String className, Table table, Expression fileFilter) {
    try {
      DynConstructors.Ctor<EvaluatorInterface> implConstructor =
                DynConstructors.builder().hiddenImpl(className, Table.class, Expression.class).buildChecked();
      return implConstructor.newInstance(table, fileFilter);
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  boolean eval(StructLike data);
}

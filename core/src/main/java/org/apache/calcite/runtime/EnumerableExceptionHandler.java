/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.runtime;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;

/**
 * Handle exceptions in the Enumerable layer.
 */
public final class EnumerableExceptionHandler {

  private EnumerableExceptionHandler() {}

  public static Expression wrapEnumerableExpression(Expression delegate) {
    try {
      return Expressions.call(
          EnumerableExceptionHandler.class.getMethod("wrapEnumerable", Enumerable.class), delegate);
    } catch (NoSuchMethodException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Wrap the initialization of an enumerator in a try/catch.
   *
   * The generated code inside of an enumerator can throw exceptions like
   * ExceptionInInitializerError which are not RuntimeExceptions and will kill the jvm.
   *
   * @param delegate, the original Enumerable
   * @param <T>, element type of the Enumerable
   * @return An enumerable which delegates to the original and provides additional
   * exception handling.
   */
  public static <T> Enumerable<T> wrapEnumerable(Enumerable<T> delegate) {
    return new AbstractEnumerable<T>() {
      @Override public Enumerator<T> enumerator() {
        try {
          return delegate.enumerator();
        } catch (Throwable t) {
          Throwable cause = t.getCause();
          if (cause != null) {
            // use the cause's message which is much more helpful than
            // e.g. ExceptionInInitializerError
            throw new RuntimeException(cause.getMessage(), t);
          }
          throw new RuntimeException(t);
        }
      }
    };
  }
}

// End EnumerableExceptionHandler.java

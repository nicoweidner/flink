/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job.savepoints;

import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.async.OperationResult;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Utility functions used in tests. */
public class SavepointTestUtilities {
    public static BiFunction<AsynchronousJobOperationKey, String, CompletableFuture<Acknowledge>>
            setReferenceToTriggerId(AtomicReference<AsynchronousJobOperationKey> key) {
        return (AsynchronousJobOperationKey operationKey, String directory) -> {
            key.set(operationKey);
            return null;
        };
    }

    public static Function<AsynchronousJobOperationKey, CompletableFuture<OperationResult<String>>>
            getResultIfKeyMatches(
                    OperationResult<String> resultToReturn,
                    AtomicReference<AsynchronousJobOperationKey> expectedKey) {
        return (AsynchronousJobOperationKey operationKey) -> {
            if (operationKey.equals(expectedKey.get())) {
                return CompletableFuture.completedFuture(resultToReturn);
            }
            throw new RuntimeException(
                    "Expected operation key "
                            + expectedKey.get()
                            + ", but received "
                            + operationKey);
        };
    }
}

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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.runtime.rest.handler.async.CompletedOperationCache;
import org.apache.flink.runtime.rest.handler.async.OperationResult;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Cache for the {@link Dispatcher} tracking ongoing and completed asynchronous operations. */
public class DispatcherCache<T> {

    private final CompletedOperationCache<AsynchronousJobOperationKey, T> operationCache =
            new CompletedOperationCache<>();

    DispatcherCache() {}

    public OperationResult<T> registerOperationIdempotently(
            AsynchronousJobOperationKey operationKey, CompletableFuture<T> operation)
            throws IllegalStateException {
        Optional<OperationResult<T>> resultOptional = operationCache.get(operationKey);
        if (resultOptional.isPresent()) {
            return resultOptional.get();
        }

        operationCache.registerOngoingOperation(operationKey, operation);
        return operationCache
                .get(operationKey)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Internal cache error: Failed to retrieve status"));
    }

    public Optional<OperationResult<T>> getOperationResult(
            AsynchronousJobOperationKey operationKey) {
        return operationCache.get(operationKey);
    }
}

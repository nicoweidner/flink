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

import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.async.CompletedOperationCache;
import org.apache.flink.runtime.rest.handler.async.OperationResult;
import org.apache.flink.runtime.rest.handler.async.OperationResultStatus;
import org.apache.flink.runtime.rest.handler.async.UnknownOperationKeyException;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A handler for async operations triggered by the {@link Dispatcher} whose trigger parameters and
 * results are cached.
 */
public class DispatcherCachedOperationsHandler {

    private final CompletedOperationCache<AsynchronousJobOperationKey, String>
            savepointTriggerCache = new CompletedOperationCache<>();

    private final Function<TriggerSavepointParameters, CompletableFuture<String>>
            triggerSavepointFunction;

    private final Function<TriggerSavepointParameters, CompletableFuture<String>>
            stopWithSavepointFunction;

    DispatcherCachedOperationsHandler(
            Function<TriggerSavepointParameters, CompletableFuture<String>>
                    triggerSavepointFunction,
            Function<TriggerSavepointParameters, CompletableFuture<String>>
                    stopWithSavepointFunction) {
        this.triggerSavepointFunction = triggerSavepointFunction;
        this.stopWithSavepointFunction = stopWithSavepointFunction;
    }

    public CompletableFuture<Acknowledge> triggerSavepoint(
            AsynchronousJobOperationKey operationKey, TriggerSavepointParameters parameters) {
        return registerOperationIdempotently(operationKey, triggerSavepointFunction, parameters);
    }

    public CompletableFuture<Acknowledge> stopWithSavepoint(
            AsynchronousJobOperationKey operationKey, TriggerSavepointParameters parameters) {
        return registerOperationIdempotently(operationKey, stopWithSavepointFunction, parameters);
    }

    public CompletableFuture<OperationResult<String>> getSavepointStatus(
            AsynchronousJobOperationKey operationKey) {
        return savepointTriggerCache
                .get(operationKey)
                .map(CompletableFuture::completedFuture)
                .orElse(
                        CompletableFuture.failedFuture(
                                new UnknownOperationKeyException(operationKey)));
    }

    private <P> CompletableFuture<Acknowledge> registerOperationIdempotently(
            AsynchronousJobOperationKey operationKey,
            Function<P, CompletableFuture<String>> operation,
            P parameters) {
        Optional<OperationResult<String>> resultOptional = savepointTriggerCache.get(operationKey);
        if (resultOptional.isPresent()) {
            return convertToFuture(resultOptional.get());
        }

        try {
            savepointTriggerCache.registerOngoingOperation(
                    operationKey, operation.apply(parameters));
        } catch (IllegalStateException e) {
            return CompletableFuture.failedFuture(e);
        }

        return savepointTriggerCache
                .get(operationKey)
                .map(this::convertToFuture)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Internal cache error: Failed to retrieve status"));
    }

    private CompletableFuture<Acknowledge> convertToFuture(OperationResult<String> result) {
        if (result.getStatus() == OperationResultStatus.FAILURE) {
            return CompletableFuture.failedFuture(result.getThrowable());
        }
        return CompletableFuture.completedFuture(Acknowledge.get());
    }
}

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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.async.CompletedOperationCache;
import org.apache.flink.runtime.rest.handler.async.OperationResult;
import org.apache.flink.runtime.rest.handler.async.UnknownOperationKeyException;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

/** Tests for the {@link DispatcherCachedOperationsHandler} component. */
public class DispatcherCachedOperationsHandlerTest extends TestLogger {

    private CompletedOperationCache<AsynchronousJobOperationKey, String> cache;
    private DispatcherCachedOperationsHandler handler;

    private SpyFunction<TriggerSavepointParameters, CompletableFuture<String>>
            triggerSavepointFunction;
    private SpyFunction<TriggerSavepointParameters, CompletableFuture<String>>
            stopWithSavepointFunction;

    private CompletableFuture<String> savepointLocationFuture = new CompletableFuture<>();
    private final TriggerSavepointParameters savepointTriggerParameters =
            new TriggerSavepointParameters(new JobID(), "dummyDirectory", false, Time.minutes(1));
    private AsynchronousJobOperationKey operationKey;

    @Before
    public void setup() {
        savepointLocationFuture = new CompletableFuture<>();
        triggerSavepointFunction = SpyFunction.wrap(parameters -> savepointLocationFuture);
        stopWithSavepointFunction = SpyFunction.wrap(parameters -> savepointLocationFuture);
        cache = new CompletedOperationCache<>();
        handler =
                new DispatcherCachedOperationsHandler(
                        triggerSavepointFunction, stopWithSavepointFunction, cache);
        operationKey =
                AsynchronousJobOperationKey.of(
                        new TriggerId(), savepointTriggerParameters.getJobID());
    }

    @Test
    public void triggerSavepointRepeatedly() throws ExecutionException, InterruptedException {
        CompletableFuture<Acknowledge> firstAcknowledge =
                handler.triggerSavepoint(operationKey, savepointTriggerParameters);
        CompletableFuture<Acknowledge> secondAcknowledge =
                handler.triggerSavepoint(operationKey, savepointTriggerParameters);

        assertThat(triggerSavepointFunction.getNumberOfInvocations(), is(1));
        assertThat(
                triggerSavepointFunction.getInvocationParameters().get(0),
                is(savepointTriggerParameters));

        assertThat(firstAcknowledge.get(), is(Acknowledge.get()));
        assertThat(secondAcknowledge.get(), is(Acknowledge.get()));
    }

    @Test
    public void stopWithSavepointRepeatedly() throws ExecutionException, InterruptedException {
        CompletableFuture<Acknowledge> firstAcknowledge =
                handler.stopWithSavepoint(operationKey, savepointTriggerParameters);
        CompletableFuture<Acknowledge> secondAcknowledge =
                handler.stopWithSavepoint(operationKey, savepointTriggerParameters);

        assertThat(stopWithSavepointFunction.getNumberOfInvocations(), is(1));
        assertThat(
                stopWithSavepointFunction.getInvocationParameters().get(0),
                is(savepointTriggerParameters));

        assertThat(firstAcknowledge.get(), is(Acknowledge.get()));
        assertThat(secondAcknowledge.get(), is(Acknowledge.get()));
    }

    @Test
    public void returnsFailedFutureIfOperationFails()
            throws ExecutionException, InterruptedException {
        CompletableFuture<Acknowledge> acknowledgeRegisteredOperation =
                handler.triggerSavepoint(operationKey, savepointTriggerParameters);
        savepointLocationFuture.completeExceptionally(new RuntimeException("Expected failure"));
        CompletableFuture<Acknowledge> failedAcknowledgeFuture =
                handler.triggerSavepoint(operationKey, savepointTriggerParameters);

        assertThat(acknowledgeRegisteredOperation.get(), is(Acknowledge.get()));
        assertThrows(ExecutionException.class, failedAcknowledgeFuture::get);
    }

    @Test
    public void returnsFailedFutureIfCacheIsShuttingDown() throws InterruptedException {
        cache.closeAsync();
        CompletableFuture<Acknowledge> returnedFuture =
                handler.triggerSavepoint(operationKey, savepointTriggerParameters);

        try {
            returnedFuture.get();
            fail("Future should have completed exceptionally");
        } catch (ExecutionException e) {
            assertThat((IllegalStateException) e.getCause(), isA(IllegalStateException.class));
        }
    }

    @Test
    public void getStatus() throws ExecutionException, InterruptedException {
        handler.triggerSavepoint(operationKey, savepointTriggerParameters);

        String savepointLocation = "location";
        savepointLocationFuture.complete(savepointLocation);

        CompletableFuture<OperationResult<String>> statusFuture =
                handler.getSavepointStatus(operationKey);

        assertEquals(statusFuture.get(), OperationResult.success(savepointLocation));
    }

    @Test
    public void getStatusFailsIfKeyUnknown() throws InterruptedException {
        CompletableFuture<OperationResult<String>> statusFuture =
                handler.getSavepointStatus(operationKey);

        try {
            statusFuture.get();
            fail("Retrieving the status should have failed");
        } catch (ExecutionException e) {
            assertThat(
                    (UnknownOperationKeyException) e.getCause(),
                    isA(UnknownOperationKeyException.class));
        }
    }

    private abstract static class SpyFunction<P, R> implements Function<P, R> {

        private final List<P> invocations = new ArrayList<>();

        @Override
        public R apply(P parameters) {
            invocations.add(parameters);
            return applyWrappedFunction(parameters);
        }

        abstract R applyWrappedFunction(P parameters);

        public List<P> getInvocationParameters() {
            return invocations;
        }

        public int getNumberOfInvocations() {
            return invocations.size();
        }

        public static <P, R> SpyFunction<P, R> wrap(Function<P, R> function) {
            return new SpyFunction<>() {
                @Override
                R applyWrappedFunction(P parameters) {
                    return function.apply(parameters);
                }
            };
        }
    }
}

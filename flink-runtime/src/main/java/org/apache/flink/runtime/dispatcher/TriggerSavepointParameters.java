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

import static org.apache.flink.util.Preconditions.checkNotNull;

class TriggerSavepointParameters {
    private final JobID jobID;
    private final String targetDirectory;
    private final boolean cancelOrTerminate;
    private final Time timeout;

    TriggerSavepointParameters(
            JobID jobID, String targetDirectory, boolean cancelOrTerminate, Time timeout) {
        this.jobID = checkNotNull(jobID);
        this.targetDirectory = checkNotNull(targetDirectory);
        this.cancelOrTerminate = cancelOrTerminate;
        this.timeout = checkNotNull(timeout);
    }

    public JobID getJobID() {
        return jobID;
    }

    public String getTargetDirectory() {
        return targetDirectory;
    }

    public boolean isCancelOrTerminate() {
        return cancelOrTerminate;
    }

    public Time getTimeout() {
        return timeout;
    }

    public static Builder builder() {
        return new Builder();
    }

    static class Builder {
        private JobID jobID;
        private String targetDirectory;
        private boolean cancelOrTerminate;
        private Time timeout;

        public Builder jobID(JobID jobID) {
            this.jobID = jobID;
            return this;
        }

        public Builder targetDirectory(String targetDirectory) {
            this.targetDirectory = targetDirectory;
            return this;
        }

        public Builder cancelOrTerminate(boolean cancelOrTerminate) {
            this.cancelOrTerminate = cancelOrTerminate;
            return this;
        }

        public Builder timeout(Time timeout) {
            this.timeout = timeout;
            return this;
        }

        public TriggerSavepointParameters build() {
            return new TriggerSavepointParameters(
                    jobID, targetDirectory, cancelOrTerminate, timeout);
        }
    }
}

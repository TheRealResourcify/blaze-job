/*
 * Copyright 2018 - 2020 Blazebit.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blazebit.job;

/**
 * A testable description of a subset of job instances.
 *
 * @author Christian Beikov
 * @since 1.0.0
 */
public interface PartitionKey {

    /**
     * Returns the concrete job instance type that is described by this partition key.
     *
     * @return the concrete job instance type
     */
    default Class<? extends JobInstance<?>> getJobInstanceType() {
        return (Class<? extends JobInstance<?>>) (Class<?>) JobInstance.class;
    }

    /**
     * Returns whether the given job instance is part of this partition key.
     *
     * @param jobInstance The job instance to test
     * @return whether the given job instance is part of this partition key
     */
    boolean matches(JobInstance<?> jobInstance);

}

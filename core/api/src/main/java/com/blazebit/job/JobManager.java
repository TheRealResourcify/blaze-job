/*
 * Copyright 2018 - 2019 Blazebit.
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

import java.time.Instant;
import java.util.List;

/**
 * A manager for adding, updating and querying job instances.
 *
 * @author Christian Beikov
 * @since 1.0.0
 */
public interface JobManager {

    /**
     * Adds the given job instance and schedules it on the appropriate partition at the configured schedule time.
     *
     * @param jobInstance The job instance to add
     */
    void addJobInstance(JobInstance<?> jobInstance);

    /**
     * Updates the given job instance and possibly reschedules it on the appropriate partition at the configured schedule time.
     *
     * @param jobInstance The job instance to update
     */
    void updateJobInstance(JobInstance<?> jobInstance);

    /**
     * Returns a schedule time ordered list of job instances that need to be processed for the given partition.
     *
     * @param partition The partition number
     * @param partitionCount The partition count
     * @param limit The amount of job instances to return at most
     * @param partitionKey The partition key
     * @return The list of job instances
     */
    List<JobInstance<?>> getJobInstancesToProcess(int partition, int partitionCount, int limit, PartitionKey partitionKey);

    /**
     * Returns the next schedule at which the given partition must process job instances.
     *
     * @param partition The partition number
     * @param partitionCount The partition count
     * @param partitionKey The partition key
     * @return The next schedule
     */
    Instant getNextSchedule(int partition, int partitionCount, PartitionKey partitionKey);
}

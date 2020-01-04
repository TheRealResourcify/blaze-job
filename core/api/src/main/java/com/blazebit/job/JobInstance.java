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

import java.time.Instant;

/**
 * A schedulable job with optional support for incremental processing.
 *
 * @param <ID> The job instance id type
 * @author Christian Beikov
 * @since 1.0.0
 */
public interface JobInstance<ID> {

    /**
     * Returns the job instance id.
     *
     * @return the job instance id
     */
    ID getId();

    /**
     * Returns a key to use to determine the partition number.
     *
     * @return a key to use to determine the partition number
     */
    Long getPartitionKey();

    /**
     * Returns the state of the job instance.
     *
     * @return the state of the job instance
     */
    JobInstanceState getState();

    /**
     * Returns the number of time this job instance was deferred.
     *
     * @return the number of time this job instance was deferred
     */
    int getDeferCount();

    /**
     * Increments the defer count.
     */
    void incrementDeferCount();

    /**
     * Returns the next schedule time.
     *
     * @return the next schedule time
     */
    Instant getScheduleTime();

    /**
     * Sets the next schedule time.
     *
     * @param scheduleTime The schedule time
     */
    void setScheduleTime(Instant scheduleTime);

    /**
     * Returns the next instant this job instance should be scheduled.
     *
     * @param jobContext The job context
     * @param scheduleContext The schedule context
     * @return the next instant
     */
    default Instant nextSchedule(JobContext jobContext, ScheduleContext scheduleContext) {
        return getScheduleTime();
    }

    /**
     * Returns the instant this job instance was created.
     *
     * @return the instant this job instance was created
     */
    Instant getCreationTime();

    /**
     * Returns the instant this job instance was last executed.
     *
     * @return the instant this job instance was last executed
     */
    Instant getLastExecutionTime();

    /**
     * Sets the instant this job was last executed.
     *
     * @param lastExecutionTime the instant
     */
    void setLastExecutionTime(Instant lastExecutionTime);

    /**
     * Returns the object that was last processed by this job instance.
     *
     * @return the object that was last processed
     */
    default Object getLastProcessed() {
        return null;
    }

    /**
     * Callback when a job instance processor successfully completed a chunk.
     * An implementation that wants to support incremental execution should extract
     * the {@link JobInstanceProcessingContext#getLastProcessed()} and remember it to be returned via {@link #getLastProcessed()}.
     *
     * @param processingContext The processing context
     */
    void onChunkSuccess(JobInstanceProcessingContext<?> processingContext);

    /**
     * Returns the job configuration.
     *
     * @return the job configuration
     */
    JobConfiguration getJobConfiguration();

    /**
     * Marks the given job instance as done and passes the last job instance processor execution result.
     * After this method, {@link #getState()} should return {@link JobInstanceState#DONE}.
     *
     * @param result The last job instance processor execution result
     */
    void markDone(Object result);

    /**
     * Marks the given job instance as failed and passes the last job instance processor execution exception.
     * After this method, {@link #getState()} should return {@link JobInstanceState#FAILED}.
     *
     * @param t The job instance processor execution exception
     */
    void markFailed(Throwable t);

    /**
     * Marks the given job instance as deferred and sets a new schedule time.
     * This might happen when attempting a job instance processor execution outside of
     * the configured {@link JobConfiguration#getExecutionTimeFrames()}.
     * This method should increment the defer count, set the new schedule time
     * and call {@link #markDropped()} if the {@link JobConfiguration#getMaximumDeferCount()} is reached.
     *
     * @param newScheduleTime The new schedule time
     */
    default void markDeferred(Instant newScheduleTime) {
        incrementDeferCount();
        int maximumDeferCount = getJobConfiguration().getMaximumDeferCount();
        if (maximumDeferCount > -1 && getDeferCount() > maximumDeferCount) {
            markDropped();
        }
        setScheduleTime(newScheduleTime);
    }

    /**
     * Marks the given job instance as deadline reached which happens when a configured {@link JobConfiguration#getDeadline()} is reached.
     * After this method, {@link #getState()} should return {@link JobInstanceState#DEADLINE_REACHED}.
     */
    void markDeadlineReached();

    /**
     * Marks the given job instance as dropped which happens when a configured {@link JobConfiguration#getMaximumDeferCount()} is reached.
     * After this method, {@link #getState()} should return {@link JobInstanceState#DROPPED}.
     */
    void markDropped();
}

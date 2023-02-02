/*
 * Copyright 2018 - 2023 Blazebit.
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
 * An abstract description of a job. A {@link JobTrigger} will refer to a job for execution.
 *
 * @author Christian Beikov
 * @since 1.0.0
 */
public interface Job {

    /**
     * Returns the id of the job.
     *
     * @return the id of the job
     */
    Long getId();

    /**
     * Returns the name of the job.
     *
     * @return the name of the job
     */
    String getName();

    /**
     * Returns the job configuration.
     *
     * @return the job configuration
     */
    JobConfiguration getJobConfiguration();

    /**
     * Returns the creation time of the job.
     *
     * @return the creation time of the job
     */
    Instant getCreationTime();

}

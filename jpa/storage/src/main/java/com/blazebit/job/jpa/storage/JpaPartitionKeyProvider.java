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
package com.blazebit.job.jpa.storage;

import com.blazebit.job.ConfigurationSource;
import com.blazebit.job.JobException;
import com.blazebit.job.JobInstance;
import com.blazebit.job.JobInstanceState;
import com.blazebit.job.JobTrigger;
import com.blazebit.job.PartitionKey;
import com.blazebit.job.ServiceProvider;
import com.blazebit.job.jpa.model.JpaPartitionKey;
import com.blazebit.job.spi.PartitionKeyProvider;

import javax.persistence.EntityManager;
import javax.persistence.metamodel.EntityType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

/**
 * A {@link PartitionKeyProvider} implementation that makes use of the JPA metamodel and configuration attributes for creating relevant {@link JpaPartitionKey}s.
 *
 * @author Christian Beikov
 * @since 1.0.0
 */
public class JpaPartitionKeyProvider implements PartitionKeyProvider {

    /**
     * Configuration property for the job trigger id attribute name.
     * The default value is "id".
     */
    public static final String JOB_TRIGGER_ID_ATTRIBUTE_NAME_PROPERTY = "job.jpa.storage.job_trigger_id_attribute_name";
    /**
     * Configuration property for the job trigger schedule attribute name.
     * The default value is "scheduleTime".
     */
    public static final String JOB_TRIGGER_SCHEDULE_ATTRIBUTE_NAME_PROPERTY = "job.jpa.storage.job_trigger_schedule_attribute_name";
    /**
     * Configuration property for the job trigger state attribute name.
     * The default value is "state".
     */
    public static final String JOB_TRIGGER_STATE_ATTRIBUTE_NAME_PROPERTY = "job.jpa.storage.job_trigger_state_attribute_name";
    /**
     * Configuration property for the job trigger state ready value.
     * The default value is "state".
     */
    public static final String JOB_TRIGGER_STATE_READY_VALUE_PROPERTY = "job.jpa.storage.job_trigger_state_ready_value";
    /**
     * Configuration property for the job instance id attribute name.
     * The default value is {@link JobInstanceState#NEW}.
     */
    public static final String JOB_INSTANCE_ID_ATTRIBUTE_NAME_PROPERTY = "job.jpa.storage.job_instance_id_attribute_name";
    /**
     * Configuration property for the job instance partition key attribute name.
     * The default value is "id".
     */
    public static final String JOB_INSTANCE_PARTITION_KEY_ATTRIBUTE_NAME_PROPERTY = "job.jpa.storage.job_instance_partition_key_attribute_name";
    /**
     * Configuration property for the job instance schedule attribute name.
     * The default value is "scheduleTime".
     */
    public static final String JOB_INSTANCE_SCHEDULE_ATTRIBUTE_NAME_PROPERTY = "job.jpa.storage.job_instance_schedule_attribute_name";
    /**
     * Configuration property for the job instance state attribute name.
     * The default value is "state".
     */
    public static final String JOB_INSTANCE_STATE_ATTRIBUTE_NAME_PROPERTY = "job.jpa.storage.job_instance_state_attribute_name";
    /**
     * Configuration property for the job instance state ready value.
     * The default value is {@link JobInstanceState#NEW}.
     */
    public static final String JOB_INSTANCE_STATE_READY_VALUE_PROPERTY = "job.jpa.storage.job_instance_state_ready_value";

    private final Collection<PartitionKey> jobTriggerPartitionKeys;
    private final Collection<PartitionKey> jobInstancePartitionKeys;

    /**
     * Creates a new partition key provider that makes use of the service provider and configuration source to determine the {@link EntityManager} and attribute names.
     *
     * @param serviceProvider     The service provider
     * @param configurationSource The configuration source
     */
    public JpaPartitionKeyProvider(ServiceProvider serviceProvider, ConfigurationSource configurationSource) {
        this(
            serviceProvider.getService(EntityManager.class),
            configurationSource.getPropertyOrDefault(JOB_TRIGGER_ID_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "id"),
            configurationSource.getPropertyOrDefault(JOB_TRIGGER_SCHEDULE_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "scheduleTime"),
            configurationSource.getPropertyOrDefault(JOB_TRIGGER_STATE_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "state"),
            configurationSource.getPropertyOrDefault(JOB_TRIGGER_STATE_READY_VALUE_PROPERTY, Object.class, null, o -> JobInstanceState.NEW),
            configurationSource.getPropertyOrDefault(JOB_INSTANCE_ID_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "id"),
            configurationSource.getPropertyOrDefault(JOB_INSTANCE_PARTITION_KEY_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "id"),
            configurationSource.getPropertyOrDefault(JOB_INSTANCE_SCHEDULE_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "scheduleTime"),
            configurationSource.getPropertyOrDefault(JOB_INSTANCE_STATE_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "state"),
            configurationSource.getPropertyOrDefault(JOB_INSTANCE_STATE_READY_VALUE_PROPERTY, Object.class, null, o -> JobInstanceState.NEW)
        );
    }

    /**
     * Creates a new partition key provider.
     *
     * @param entityManager                        The entity manager
     * @param jobTriggerIdAttributeName            The trigger id attribute name
     * @param jobTriggerScheduleAttributeName      The trigger schedule attribute name
     * @param jobTriggerStateAttributeName         The trigger state attribute name
     * @param jobTriggerStateReadyValue            The trigger state ready value
     * @param jobInstanceIdAttributeName           The job instance id attribute name
     * @param jobInstancePartitionKeyAttributeName The job instance partition key attribute name
     * @param jobInstanceScheduleAttributeName     The job instance schedule attribute name
     * @param jobInstanceStateAttributeName        The job instance state attribute name
     * @param jobInstanceStateReadyValue           The job instance state ready value
     */
    public JpaPartitionKeyProvider(EntityManager entityManager, String jobTriggerIdAttributeName, String jobTriggerScheduleAttributeName, String jobTriggerStateAttributeName, Object jobTriggerStateReadyValue,
                                   String jobInstanceIdAttributeName, String jobInstancePartitionKeyAttributeName, String jobInstanceScheduleAttributeName, String jobInstanceStateAttributeName, Object jobInstanceStateReadyValue) {
        if (entityManager == null) {
            throw new JobException("No entity manager given!");
        }

        Collection<PartitionKey> jobTriggerPartitionKeys = new ArrayList<>();
        Collection<PartitionKey> jobInstancePartitionKeys = new ArrayList<>();
        for (EntityType<?> entity : entityManager.getMetamodel().getEntities()) {
            Class<?> javaType = entity.getJavaType();
            if (javaType != null) {
                if (JobTrigger.class.isAssignableFrom(javaType)) {
                    jobTriggerPartitionKeys.add(
                        JpaPartitionKey.builder()
                            .withName(entity.getName())
                            .withJobInstanceType((Class<? extends JobInstance<?>>) javaType)
                            .withIdAttributeName(jobTriggerIdAttributeName)
                            .withScheduleAttributeName(jobTriggerScheduleAttributeName)
                            .withPartitionKeyAttributeName(jobTriggerIdAttributeName)
                            .withStateAttributeName(jobTriggerStateAttributeName)
                            .withReadyStateValue(jobTriggerStateReadyValue)
                            .build()
                    );
                } else if (JobInstance.class.isAssignableFrom(javaType)) {
                    jobInstancePartitionKeys.add(
                        JpaPartitionKey.builder()
                            .withName(entity.getName())
                            .withJobInstanceType((Class<? extends JobInstance<?>>) javaType)
                            .withIdAttributeName(jobInstanceIdAttributeName)
                            .withScheduleAttributeName(jobInstanceScheduleAttributeName)
                            .withPartitionKeyAttributeName(jobInstancePartitionKeyAttributeName)
                            .withStateAttributeName(jobInstanceStateAttributeName)
                            .withReadyStateValue(jobInstanceStateReadyValue)
                            .build()
                    );
                }
            }
        }
        this.jobTriggerPartitionKeys = jobTriggerPartitionKeys;
        this.jobInstancePartitionKeys = jobInstancePartitionKeys;
    }

    @Override
    public Collection<PartitionKey> getDefaultTriggerPartitionKeys() {
        return jobTriggerPartitionKeys;
    }

    @Override
    public Collection<PartitionKey> getDefaultJobInstancePartitionKeys() {
        return jobInstancePartitionKeys;
    }
}

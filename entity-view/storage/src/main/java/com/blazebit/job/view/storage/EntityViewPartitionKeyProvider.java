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
package com.blazebit.job.view.storage;

import com.blazebit.job.ConfigurationSource;
import com.blazebit.job.JobException;
import com.blazebit.job.JobInstance;
import com.blazebit.job.JobInstanceState;
import com.blazebit.job.JobTrigger;
import com.blazebit.job.PartitionKey;
import com.blazebit.job.ServiceProvider;
import com.blazebit.job.spi.PartitionKeyProvider;
import com.blazebit.job.view.model.AbstractJobInstance;
import com.blazebit.job.view.model.EntityViewPartitionKey;
import com.blazebit.persistence.CriteriaBuilderFactory;
import com.blazebit.persistence.view.EntityViewManager;
import com.blazebit.persistence.view.metamodel.ViewType;

import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A {@link PartitionKeyProvider} implementation that makes use of the JPA metamodel and configuration attributes for creating relevant {@link EntityViewPartitionKey}s.
 *
 * @author Christian Beikov
 * @since 1.0.0
 */
public class EntityViewPartitionKeyProvider implements PartitionKeyProvider {

    /**
     * Configuration property for the job trigger id attribute name.
     * The default value is "id".
     */
    public static final String JOB_TRIGGER_ID_ATTRIBUTE_NAME_PROPERTY = "job.view.storage.job_trigger_id_attribute_name";
    /**
     * Configuration property for the job trigger schedule attribute name.
     * The default value is "scheduleTime".
     */
    public static final String JOB_TRIGGER_SCHEDULE_ATTRIBUTE_NAME_PROPERTY = "job.view.storage.job_trigger_schedule_attribute_name";
    /**
     * Configuration property for the job trigger last execution time attribute name.
     * The default value is "lastExecutionTime".
     */
    public static final String JOB_TRIGGER_LAST_EXECUTION_ATTRIBUTE_NAME_PROPERTY = "job.view.storage.job_trigger_last_execution_attribute_name";
    /**
     * Configuration property for the job trigger state attribute name.
     * The default value is "state".
     */
    public static final String JOB_TRIGGER_STATE_ATTRIBUTE_NAME_PROPERTY = "job.view.storage.job_trigger_state_attribute_name";
    /**
     * Configuration property for a mapping function <code>Function&lt;JobInstanceState, Object&gt;</code> from job trigger state to the actual model state.
     * The default value is a function that just returns the passed in {@link JobInstanceState}.
     */
    public static final String JOB_TRIGGER_STATE_VALUE_MAPPING_FUNCTION_PROPERTY = "job.view.storage.job_trigger_state_value_mapping_function";
    /**
     * Configuration property for the job instance id attribute name.
     * The default value is "id".
     */
    public static final String JOB_INSTANCE_ID_ATTRIBUTE_NAME_PROPERTY = "job.view.storage.job_instance_id_attribute_name";
    /**
     * Configuration property for the job instance partition key attribute name.
     * The default value is "id".
     */
    public static final String JOB_INSTANCE_PARTITION_KEY_ATTRIBUTE_NAME_PROPERTY = "job.view.storage.job_instance_partition_key_attribute_name";
    /**
     * Configuration property for the job instance schedule attribute name.
     * The default value is "scheduleTime".
     */
    public static final String JOB_INSTANCE_SCHEDULE_ATTRIBUTE_NAME_PROPERTY = "job.view.storage.job_instance_schedule_attribute_name";
    /**
     * Configuration property for the job instance last execution time attribute name.
     * The default value is "lastExecutionTime".
     */
    public static final String JOB_INSTANCE_LAST_EXECUTION_ATTRIBUTE_NAME_PROPERTY = "job.view.storage.job_instance_last_execution_attribute_name";
    /**
     * Configuration property for the job instance state attribute name.
     * The default value is "state".
     */
    public static final String JOB_INSTANCE_STATE_ATTRIBUTE_NAME_PROPERTY = "job.view.storage.job_instance_state_attribute_name";
    /**
     * Configuration property for a mapping function <code>Function&lt;JobInstanceState, Object&gt;</code> from job instance state to the actual model state.
     * The default value is a function that just returns the passed in {@link JobInstanceState}.
     */
    public static final String JOB_INSTANCE_STATE_VALUE_MAPPING_FUNCTION_PROPERTY = "job.view.storage.job_instance_state_value_mapping_function";

    private final Collection<PartitionKey> jobTriggerPartitionKeys;
    private final Collection<PartitionKey> jobInstancePartitionKeys;

    /**
     * Creates a new partition key provider that makes use of the service provider and configuration source to determine the {@link EntityViewManager} and attribute names.
     *
     * @param serviceProvider     The service provider
     * @param configurationSource The configuration source
     */
    public EntityViewPartitionKeyProvider(ServiceProvider serviceProvider, ConfigurationSource configurationSource) {
        this(
            serviceProvider.getService(CriteriaBuilderFactory.class),
            serviceProvider.getService(EntityViewManager.class),
            configurationSource.getPropertyOrDefault(JOB_TRIGGER_ID_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "id"),
            configurationSource.getPropertyOrDefault(JOB_TRIGGER_SCHEDULE_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "scheduleTime"),
            configurationSource.getPropertyOrDefault(JOB_TRIGGER_LAST_EXECUTION_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "lastExecutionTime"),
            configurationSource.getPropertyOrDefault(JOB_TRIGGER_STATE_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "state"),
            configurationSource.getPropertyOrDefault(JOB_TRIGGER_STATE_VALUE_MAPPING_FUNCTION_PROPERTY, Function.class, null, o -> Function.identity()),
            configurationSource.getPropertyOrDefault(JOB_INSTANCE_ID_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "id"),
            configurationSource.getPropertyOrDefault(JOB_INSTANCE_PARTITION_KEY_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "id"),
            configurationSource.getPropertyOrDefault(JOB_INSTANCE_SCHEDULE_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "scheduleTime"),
            configurationSource.getPropertyOrDefault(JOB_INSTANCE_LAST_EXECUTION_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "lastExecutionTime"),
            configurationSource.getPropertyOrDefault(JOB_INSTANCE_STATE_ATTRIBUTE_NAME_PROPERTY, String.class, Function.identity(), o -> "state"),
            configurationSource.getPropertyOrDefault(JOB_INSTANCE_STATE_VALUE_MAPPING_FUNCTION_PROPERTY, Function.class, null, o -> Function.identity())
        );
    }

    /**
     * Creates a new partition key provider.
     *
     * @param criteriaBuilderFactory                The criteria builder factory
     * @param entityViewManager                     The entity view manager
     * @param jobTriggerIdAttributeName             The trigger id attribute name
     * @param jobTriggerScheduleAttributeName       The trigger schedule attribute name
     * @param jobTriggerLastExecutionAttributeName  The trigger last execution attribute name
     * @param jobTriggerStateAttributeName          The trigger state attribute name
     * @param jobTriggerStateValueMapper            The trigger state value mapping function
     * @param jobInstanceIdAttributeName            The job instance id attribute name
     * @param jobInstancePartitionKeyAttributeName  The job instance partition key attribute name
     * @param jobInstanceScheduleAttributeName      The job instance schedule attribute name
     * @param jobInstanceLastExecutionAttributeName The job instance last execution attribute name
     * @param jobInstanceStateAttributeName         The job instance state attribute name
     * @param jobInstanceStateValueMapper           The job instance state value mapping function
     */
    public EntityViewPartitionKeyProvider(CriteriaBuilderFactory criteriaBuilderFactory, EntityViewManager entityViewManager, String jobTriggerIdAttributeName, String jobTriggerScheduleAttributeName, String jobTriggerLastExecutionAttributeName, String jobTriggerStateAttributeName, Function<JobInstanceState, Object> jobTriggerStateValueMapper,
                                          String jobInstanceIdAttributeName, String jobInstancePartitionKeyAttributeName, String jobInstanceScheduleAttributeName, String jobInstanceLastExecutionAttributeName, String jobInstanceStateAttributeName, Function<JobInstanceState, Object> jobInstanceStateValueMapper) {
        if (entityViewManager == null) {
            throw new JobException("No entity view manager given!");
        }
        Collection<PartitionKey> jobTriggerPartitionKeys = new ArrayList<>();
        Collection<PartitionKey> jobInstancePartitionKeys = new ArrayList<>();
        Map<EntityType<?>, List<EntityType<?>>> entitySubtypeMap = new HashMap<>();
        Metamodel metamodel = criteriaBuilderFactory.getService(Metamodel.class);
        for (EntityType<?> entity : metamodel.getEntities()) {
            Class<?> javaType = entity.getJavaType();
            // We only query non-abstract entity types
            if (javaType != null && !Modifier.isAbstract(javaType.getModifiers())) {
                if (JobTrigger.class.isAssignableFrom(javaType) || JobInstance.class.isAssignableFrom(javaType)) {
                    List<EntityType<?>> subtypes = new ArrayList<>();
                    entitySubtypeMap.put(entity, subtypes);
                    while (entity.getSupertype() instanceof EntityType<?>) {
                        EntityType<?> supertype = (EntityType<?>) entity.getSupertype();
                        Class<?> supertypeJavaType = supertype.getJavaType();
                        if (supertypeJavaType != null && !Modifier.isAbstract(supertypeJavaType.getModifiers())) {
                            List<EntityType<?>> superSubtypes = entitySubtypeMap.compute(supertype, (e, list) -> list == null ? new ArrayList<>() : list);
                            superSubtypes.add(entity);
                            if (subtypes != null) {
                                superSubtypes.addAll(subtypes);
                            }

                            entity = supertype;
                            subtypes = entitySubtypeMap.get(entity);
                        } else {
                            entity = supertype;
                            if (subtypes.isEmpty()) {
                                subtypes = entitySubtypeMap.get(entity);
                            } else {
                                // We propagate all subtypes up to non-abstract supertypes
                                subtypes = new ArrayList<>(subtypes);
                                subtypes.addAll(entitySubtypeMap.get(entity));
                            }
                        }
                    }
                }
            }
        }

        Map<Class<?>, List<ViewType<?>>> eligibleViewTypes = new HashMap<>();
        List<ViewType<?>> possibleDefaultViewTypes = new ArrayList<>();

        for (ViewType<?> view : entityViewManager.getMetamodel().getViews()) {
            if (view.isUpdatable() && AbstractJobInstance.class.isAssignableFrom(view.getJavaType())) {
                if (entitySubtypeMap.containsKey(metamodel.entity(view.getEntityClass()))) {
                    eligibleViewTypes.computeIfAbsent(view.getEntityClass(), k -> new ArrayList<>()).add(view);
                } else {
                    possibleDefaultViewTypes.add(view);
                }
            }
        }

        List<String> errors = new ArrayList<>();
        for (Map.Entry<EntityType<?>, List<EntityType<?>>> entry : entitySubtypeMap.entrySet()) {
            EntityType<?> entity = entry.getKey();
            Class<?> entityJavaType = entity.getJavaType();
            Function<String, String> partitionKeyPredicateProvider;
            ViewType<?> viewType;
            List<ViewType<?>> viewTypes = eligibleViewTypes.get(entityJavaType);
            if (viewTypes == null || viewTypes.size() > 1) {
                if (possibleDefaultViewTypes.size() == 0) {
                    if (viewTypes != null && viewTypes.size() > 1) {
                        errors.add("Could not determine the entity view for the job instance entity type '" + entityJavaType.getName() + "' because there are multiple no possible default view types but multiple possible view types: " + viewTypes);
                    } else {
                        errors.add("Could not determine the entity view for the job instance entity type '" + entityJavaType.getName() + "' because there are multiple no possible default view types!");
                    }
                    continue;
                }
                if (possibleDefaultViewTypes.size() > 1) {
                    if (viewTypes != null && viewTypes.size() > 1) {
                        errors.add("Could not determine the entity view for the job instance entity type '" + entityJavaType.getName() + "' because there are multiple possible default view types: " + possibleDefaultViewTypes + " and multiple possible view types: " + viewTypes);
                    } else {
                        errors.add("Could not determine the entity view for the job instance entity type '" + entityJavaType.getName() + "' because there are multiple possible default view types: " + possibleDefaultViewTypes);
                    }
                    continue;
                }
                viewType = possibleDefaultViewTypes.get(0);
                partitionKeyPredicateProvider = alias -> "TYPE(" + alias + ") = " + entity.getName();
            } else {
                viewType = viewTypes.get(0);
                if (entry.getValue().isEmpty()) {
                    partitionKeyPredicateProvider = null;
                } else {
                    partitionKeyPredicateProvider = alias -> "TYPE(" + alias + ") = " + entity.getName();
                }
            }

            if (JobTrigger.class.isAssignableFrom(entityJavaType)) {
                jobTriggerPartitionKeys.add(
                    EntityViewPartitionKey.builder()
                        .withName(entity.getName())
                        .withEntityClass(entityJavaType)
                        .withJobInstanceType((Class<? extends JobInstance<?>>) viewType.getJavaType())
                        .withPartitionPredicateProvider(partitionKeyPredicateProvider)
                        .withIdAttributeName(jobTriggerIdAttributeName)
                        .withScheduleAttributeName(jobTriggerScheduleAttributeName)
                        .withLastExecutionAttributeName(jobTriggerLastExecutionAttributeName)
                        .withPartitionKeyAttributeName(jobTriggerIdAttributeName)
                        .withStateAttributeName(jobTriggerStateAttributeName)
                        .withStateValueMappingFunction(jobTriggerStateValueMapper)
                        .build()
                );
            } else if (JobInstance.class.isAssignableFrom(entityJavaType)) {
                jobInstancePartitionKeys.add(
                    EntityViewPartitionKey.builder()
                        .withName(entity.getName())
                        .withEntityClass(entityJavaType)
                        .withJobInstanceType((Class<? extends JobInstance<?>>) viewType.getJavaType())
                        .withPartitionPredicateProvider(partitionKeyPredicateProvider)
                        .withIdAttributeName(jobInstanceIdAttributeName)
                        .withScheduleAttributeName(jobInstanceScheduleAttributeName)
                        .withLastExecutionAttributeName(jobInstanceLastExecutionAttributeName)
                        .withPartitionKeyAttributeName(jobInstancePartitionKeyAttributeName)
                        .withStateAttributeName(jobInstanceStateAttributeName)
                        .withStateValueMappingFunction(jobInstanceStateValueMapper)
                        .build()
                );
            }
        }
        if (!errors.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("There were errors while determining the partition keys for the entity view model:");
            for (String error : errors) {
                sb.append("\n\t- ").append(error);
            }

            throw new JobException(sb.toString());
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

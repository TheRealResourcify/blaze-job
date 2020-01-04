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

package com.blazebit.job.jpa.model;

import com.blazebit.job.JobInstance;
import com.blazebit.job.JobInstanceState;
import com.blazebit.job.PartitionKey;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * A description of a subset of job instances that can be applied to a query.
 *
 * @author Christian Beikov
 * @since 1.0.0
 */
public interface JpaPartitionKey extends PartitionKey {

    /**
     * A JPQL predicate for filtering for this partition.
     * The job alias refers to an entity of the type as given in {@link #getJobInstanceType()}.
     *
     * @param jobAlias The FROM clause alias for the job
     * @return The partition JPQL predicate or an empty string
     */
    String getPartitionPredicate(String jobAlias);

    /**
     * Returns the attribute name of the identifier attribute of the entity type as given in {@link #getJobInstanceType()}.
     *
     * @return the attribute name of the identifier attribute
     */
    String getIdAttributeName();

    /**
     * Returns the attribute name of the schedule attribute of the entity type as given in {@link #getJobInstanceType()}.
     *
     * @return the attribute name of the schedule attribute
     */
    String getScheduleAttributeName();

    /**
     * Returns the attribute name of the last execution attribute of the entity type as given in {@link #getJobInstanceType()}.
     *
     * @return the attribute name of the last execution attribute
     */
    String getLastExecutionAttributeName();

    /**
     * Returns the attribute name of the partition key attribute of the entity type as given in {@link #getJobInstanceType()}.
     *
     * @return the attribute name of the partition key attribute
     */
    String getPartitionKeyAttributeName();

    /**
     * Returns the predicate for matching only jobs that are ready to process.
     *
     * @param jobAlias The FROM clause alias for the job
     * @return The state JPQL predicate or an empty string
     */
    String getStatePredicate(String jobAlias);

    /**
     * Returns the expression for the state of a job.
     *
     * @param jobAlias The FROM clause alias for the job
     * @return The state JPQL expression or an empty string
     */
    String getStateExpression(String jobAlias);

    /**
     * Returns the state value for ready jobs that must be bound in a query to the parameter name "readyState".
     * A <code>null</code> value means that no parameter should be bound.
     *
     * @return The ready state value of <code>null</code>
     */
    Function<JobInstanceState, Object> getStateValueMappingFunction();

    /**
     * Returns the join fetches that should be applied to a query when fetching a job for this partition.
     *
     * @param jobAlias The FROM clause alias for the job
     * @return the join fetches or an empty string
     */
    String getJoinFetches(String jobAlias);

    @Override
    default boolean matches(JobInstance<?> jobInstance) {
        throw new UnsupportedOperationException("A JpaPartitionKey does not need to support this!");
    }

    /**
     * A builder for {@link JpaPartitionKey} instances.
     *
     * @author Christian Beikov
     * @since 1.0.0
     */
    interface JpaPartitionKeyBuilder {
        /**
         * Sets the given name as partition name.
         *
         * @param name The name
         * @return this for chaining
         */
        JpaPartitionKeyBuilder withName(String name);

        /**
         * Sets the given job instance type.
         *
         * @param jobInstanceType The job instance type
         * @return this for chaining
         */
        JpaPartitionKeyBuilder withJobInstanceType(Class<? extends JobInstance<?>> jobInstanceType);

        /**
         * Sets the given partition predicate provider.
         *
         * @param partitionPredicateProvider The partition predicate provider
         * @return this for chaining
         */
        JpaPartitionKeyBuilder withPartitionPredicateProvider(Function<String, String> partitionPredicateProvider);

        /**
         * Sets the given job id attribute name.
         *
         * @param idAttributeName The job id attribute name
         * @return this for chaining
         */
        JpaPartitionKeyBuilder withIdAttributeName(String idAttributeName);

        /**
         * Sets the given job schedule attribute name.
         *
         * @param scheduleAttributeName The job schedule attribute name
         * @return this for chaining
         */
        JpaPartitionKeyBuilder withScheduleAttributeName(String scheduleAttributeName);

        /**
         * Sets the given job last execution attribute name.
         *
         * @param lastExecutionAttributeName The job last execution attribute name
         * @return this for chaining
         */
        JpaPartitionKeyBuilder withLastExecutionAttributeName(String lastExecutionAttributeName);

        /**
         * Sets the given job partition key attribute name.
         *
         * @param partitionKeyAttributeName The job partition key attribute name
         * @return this for chaining
         */
        JpaPartitionKeyBuilder withPartitionKeyAttributeName(String partitionKeyAttributeName);

        /**
         * Sets the given job state attribute name.
         *
         * @param stateAttributeName The job state attribute name
         * @return this for chaining
         */
        JpaPartitionKeyBuilder withStateAttributeName(String stateAttributeName);

        /**
         * Sets the given state value mapping function.
         *
         * @param stateValueMappingFunction The state value mapping function
         * @return this for chaining
         */
        JpaPartitionKeyBuilder withStateValueMappingFunction(Function<JobInstanceState, Object> stateValueMappingFunction);

        /**
         * Sets the given job attributes to fetch.
         *
         * @param fetches The attributes to fetch
         * @return this for chaining
         */
        JpaPartitionKeyBuilder withJoinFetches(String... fetches);

        /**
         * Returns a new {@link JpaPartitionKey} for the configuration of this builder.
         *
         * @return the {@link JpaPartitionKey}
         */
        JpaPartitionKey build();
    }

    /**
     * Returns a new builder for a new {@link JpaPartitionKey}.
     *
     * @return the builder
     */
    static JpaPartitionKeyBuilder builder() {
        return new JpaPartitionKeyBuilder() {
            String name0;
            Class<? extends JobInstance<?>> jobInstanceType0;
            Function<String, String> partitionPredicateProvider0;
            String idAttributeName0;
            String scheduleAttributeName0;
            String lastExecutionAttributeName0;
            String partitionKeyAttributeName0;
            String stateAttributeName0;
            Function<JobInstanceState, Object> stateValueMappingFunction0;
            List<String> fetches0 = new ArrayList<>();

            @Override
            public JpaPartitionKeyBuilder withName(String name) {
                this.name0 = name;
                return this;
            }

            @Override
            public JpaPartitionKeyBuilder withJobInstanceType(Class<? extends JobInstance<?>> jobInstanceType) {
                this.jobInstanceType0 = jobInstanceType;
                return this;
            }

            @Override
            public JpaPartitionKeyBuilder withPartitionPredicateProvider(Function<String, String> partitionPredicateProvider) {
                this.partitionPredicateProvider0 = partitionPredicateProvider;
                return this;
            }

            @Override
            public JpaPartitionKeyBuilder withIdAttributeName(String idAttributeName) {
                this.idAttributeName0 = idAttributeName;
                return this;
            }

            @Override
            public JpaPartitionKeyBuilder withScheduleAttributeName(String scheduleAttributeName) {
                this.scheduleAttributeName0 = scheduleAttributeName;
                return this;
            }

            @Override
            public JpaPartitionKeyBuilder withLastExecutionAttributeName(String lastExecutionAttributeName) {
                this.lastExecutionAttributeName0 = lastExecutionAttributeName;
                return this;
            }

            @Override
            public JpaPartitionKeyBuilder withPartitionKeyAttributeName(String partitionKeyAttributeName) {
                this.partitionKeyAttributeName0 = partitionKeyAttributeName;
                return this;
            }

            @Override
            public JpaPartitionKeyBuilder withStateAttributeName(String stateAttributeName) {
                this.stateAttributeName0 = stateAttributeName;
                return this;
            }

            @Override
            public JpaPartitionKeyBuilder withStateValueMappingFunction(Function<JobInstanceState, Object> stateValueMappingFunction) {
                this.stateValueMappingFunction0 = stateValueMappingFunction;
                return this;
            }

            @Override
            public JpaPartitionKeyBuilder withJoinFetches(String... fetches) {
                Collections.addAll(this.fetches0, fetches);
                return this;
            }

            @Override
            public JpaPartitionKey build() {
                return new JpaPartitionKey() {
                    private final String name = name0;
                    private final Class<? extends JobInstance<?>> jobInstanceType = jobInstanceType0;
                    private final Function<String, String> partitionPredicateProvider = partitionPredicateProvider0;
                    private final String idAttributeName = idAttributeName0;
                    private final String scheduleAttributeName = scheduleAttributeName0;
                    private final String lastExecutionAttributeName = lastExecutionAttributeName0;
                    private final String partitionKeyAttributeName = partitionKeyAttributeName0;
                    private final String stateAttributeName = stateAttributeName0;
                    private final Function<JobInstanceState, Object> stateValueMappingFunction = stateValueMappingFunction0;
                    private final String[] fetches = fetches0.toArray(new String[fetches0.size()]);

                    @Override
                    public Class<? extends JobInstance<?>> getJobInstanceType() {
                        return jobInstanceType;
                    }

                    @Override
                    public String getPartitionPredicate(String jobAlias) {
                        return partitionPredicateProvider == null ? "" : partitionPredicateProvider.apply(jobAlias);
                    }

                    @Override
                    public String getIdAttributeName() {
                        return idAttributeName;
                    }

                    @Override
                    public String getScheduleAttributeName() {
                        return scheduleAttributeName;
                    }

                    @Override
                    public String getLastExecutionAttributeName() {
                        return lastExecutionAttributeName;
                    }

                    @Override
                    public String getPartitionKeyAttributeName() {
                        return partitionKeyAttributeName;
                    }

                    @Override
                    public String getStatePredicate(String jobAlias) {
                        return jobAlias + "." + stateAttributeName + " = :readyState";
                    }

                    @Override
                    public String getStateExpression(String jobAlias) {
                        return jobAlias + "." + stateAttributeName;
                    }

                    @Override
                    public Function<JobInstanceState, Object> getStateValueMappingFunction() {
                        return stateValueMappingFunction;
                    }

                    @Override
                    public String getJoinFetches(String jobAlias) {
                        if (fetches.length == 0) {
                            return "";
                        }
                        StringBuilder sb = new StringBuilder();
                        String previousAlias = jobAlias;
                        for (int i = 0; i < fetches.length; i++) {
                            String fetch = fetches[i];
                            sb.append(" LEFT JOIN FETCH ").append(previousAlias).append('.').append(fetch).append(' ').append(jobAlias).append(i);
                            previousAlias = jobAlias + i;
                        }

                        return sb.toString();
                    }

                    @Override
                    public String toString() {
                        return name;
                    }
                };
            }
        };
    }
}

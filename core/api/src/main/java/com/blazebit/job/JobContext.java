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

import com.blazebit.actor.ActorContext;
import com.blazebit.actor.ActorContextBuilder;
import com.blazebit.job.spi.JobInstanceProcessorFactory;
import com.blazebit.job.spi.JobManagerFactory;
import com.blazebit.job.spi.JobProcessorFactory;
import com.blazebit.job.spi.JobScheduler;
import com.blazebit.job.spi.JobSchedulerFactory;
import com.blazebit.job.spi.PartitionKeyProvider;
import com.blazebit.job.spi.PartitionKeyProviderFactory;
import com.blazebit.job.spi.ScheduleFactory;
import com.blazebit.job.spi.TransactionSupport;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A closable context in which jobs run.
 *
 * @author Christian Beikov
 * @since 1.0.0
 */
public interface JobContext extends ServiceProvider, ConfigurationSource {

    /**
     * Returns the transaction support for this job context.
     *
     * @return the transaction support
     */
    TransactionSupport getTransactionSupport();

    /**
     * Returns the job manager.
     *
     * @return the job manager
     */
    JobManager getJobManager();

    /**
     * Returns the schedule factory.
     *
     * @return the schedule factory
     */
    ScheduleFactory getScheduleFactory();

    /**
     * Returns the job processor for the given job trigger.
     *
     * @param job The job trigger
     * @param <T> The job trigger type
     * @return The job processor
     * @throws JobException if the job processor can't be created
     */
    <T extends JobTrigger> JobProcessor<T> getJobProcessor(T job);

    /**
     * Returns the job instance processor for the given job instance.
     *
     * @param job The job instance
     * @param <T> The job instance type
     * @return The job instance processor
     * @throws JobException if the job instance processor can't be created
     */
    <T extends JobInstance<?>> JobInstanceProcessor<?, T> getJobInstanceProcessor(T job);

    /**
     * Returns all partition keys.
     *
     * @return The list of all partition keys
     */
    Collection<PartitionKey> getPartitionKeys();

    /**
     * Returns the matching partition keys for the given job instance.
     *
     * @param jobInstance The job instance
     * @return The list of matching partition keys
     */
    Collection<PartitionKey> getPartitionKeys(JobInstance<?> jobInstance);

    /**
     * Refreshes the job instance schedules for the given job instance.
     *
     * @param jobInstance The job instance for which to refresh the schedules
     */
    void refreshJobInstanceSchedules(JobInstance<?> jobInstance);

    /**
     * Refreshes the overall job instance schedules based on the given new earliest schedule.
     *
     * @param earliestNewSchedule The new earliest schedule
     */
    void refreshJobInstanceSchedules(long earliestNewSchedule);

    /**
     * Refreshes the overall job instance schedules for the given partition based on the given new earliest schedule.
     *
     * @param partitionKey The partition for which to refresh the schedules
     * @param earliestNewSchedule The new earliest schedule
     */
    void refreshJobInstanceSchedules(PartitionKey partitionKey, long earliestNewSchedule);

    /**
     * Calls the given consumer for every job instance listener that is registered.
     *
     * @param jobInstanceListenerConsumer The consumer for job instance listeners
     */
    void forEachJobInstanceListeners(Consumer<JobInstanceListener> jobInstanceListenerConsumer);

    /**
     * Stops the job context.
     * After this method finished no further jobs are scheduled but there may still be running jobs.
     */
    void stop();

    /**
     * Stops the job context and waits up to the given amount of time for currently running jobs to finish.
     * After this method finished no further jobs are scheduled.
     *
     * @param timeout The maximum time to wait
     * @param unit The time unit of the timeout argument
     * @throws InterruptedException if interrupted while waiting
     */
    void stop(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Returns a builder for a job context.
     *
     * @return a builder for a job context
     */
    static Builder builder() {
        Builder builder = new Builder();
        builder.loadDefaults();
        return builder;
    }
    /**
     * The builder for a plain job context.
     *
     * @author Christian Beikov
     * @since 1.0.0
     */
    class Builder extends BuilderBase<Builder> {
    }

    /**
     * A base builder that sub-projects can extend to build a custom job context.
     *
     * @param <T> The concrete builder type
     * @author Christian Beikov
     * @since 1.0.0
     */
    class BuilderBase<T extends BuilderBase<T>> {

        private TransactionSupport transactionSupport;
        private ActorContext actorContext;
        private ActorContextBuilder actorContextBuilder;
        private JobManagerFactory jobManagerFactory;
        private ScheduleFactory scheduleFactory;
        private JobSchedulerFactory jobSchedulerFactory;
        private JobProcessorFactory jobProcessorFactory;
        private JobInstanceProcessorFactory jobInstanceProcessorFactory;
        private PartitionKeyProviderFactory partitionKeyProviderFactory;
        private PartitionKeyProvider partitionKeyProvider;
        private final Map<PartitionKey, Integer> partitionKeys = new HashMap<>();
        private final List<JobTriggerListener> jobTriggerListeners = new ArrayList<>();
        private final List<JobInstanceListener> jobInstanceListeners = new ArrayList<>();
        private final Map<String, Object> properties = new HashMap<>();
        private final Map<Class<?>, Object> serviceMap = new HashMap<>();

        /**
         * Loads the default services via the {@link ServiceLoader} API.
         */
        protected void loadDefaults() {
            transactionSupport = loadFirstServiceOrNone(TransactionSupport.class);
            if (transactionSupport == null) {
                transactionSupport = TransactionSupport.NOOP;
            }

            jobManagerFactory = loadFirstServiceOrNone(JobManagerFactory.class);
            scheduleFactory = loadFirstServiceOrNone(ScheduleFactory.class);
            jobSchedulerFactory = loadFirstServiceOrNone(JobSchedulerFactory.class);
            jobProcessorFactory = loadFirstServiceOrNone(JobProcessorFactory.class);
            jobInstanceProcessorFactory = loadFirstServiceOrNone(JobInstanceProcessorFactory.class);
            partitionKeyProviderFactory = loadFirstServiceOrNone(PartitionKeyProviderFactory.class);

            jobTriggerListeners.addAll(loadServices(JobTriggerListener.class));
            jobInstanceListeners.addAll(loadServices(JobInstanceListener.class));
        }

        /**
         * Loads the first service that is found via the {@link ServiceLoader} API or <code>null</code> if none or multiple are found.
         *
         * @param serviceClass The service type
         * @param <X> The service type
         * @return The service
         */
        protected static <X> X loadFirstServiceOrNone(Class<X> serviceClass) {
            Iterator<X> scheduleFactoryIterator = ServiceLoader.load(serviceClass).iterator();
            if (scheduleFactoryIterator.hasNext()) {
                X o = scheduleFactoryIterator.next();
                if (scheduleFactoryIterator.hasNext()) {
                    return null;
                }
                return o;
            }
            return null;
        }

        /**
         * Loads all services that are found via the {@link ServiceLoader} API as list.
         *
         * @param serviceClass The service type
         * @param <X> The service type
         * @return The services
         */
        protected static <X> List<X> loadServices(Class<X> serviceClass) {
            List<X> list = new ArrayList<>();
            for (X service : ServiceLoader.load(serviceClass)) {
                list.add(service);
            }
            return list;
        }

        /**
         * Sanity checks for creating a context.
         */
        protected void checkCreateContext() {
            if (getTransactionSupport() == null) {
                throw new JobException("No transaction support given!");
            }
            if (getJobManagerFactory() == null) {
                throw new JobException("No job manager factory given!");
            }
            if (getScheduleFactory() == null) {
                throw new JobException("No schedule factory given!");
            }
            if (getJobSchedulerFactory() == null) {
                throw new JobException("No job scheduler factory given!");
            }
            if (getJobProcessorFactory() == null) {
                throw new JobException("No job processor factory given!");
            }
            if (getJobInstanceProcessorFactory() == null) {
                throw new JobException("No job instance processor factory given!");
            }
            if (getPartitionKeyProviderFactory() == null) {
                throw new JobException("No job instance partition key provider factory given!");
            }
        }

        /**
         * Returns a new job context.
         *
         * @return a new job context
         */
        public JobContext createContext() {
            checkCreateContext();
            return new DefaultJobContext(
                    transactionSupport,
                    getJobManagerFactory(),
                    getOrCreateActorContext(),
                    getScheduleFactory(),
                    getJobSchedulerFactory(),
                    getJobProcessorFactory(),
                    getJobInstanceProcessorFactory(),
                    getPartitionKeyMap(),
                    getPartitionKeyProvider(),
                    getJobTriggerListeners(),
                    getJobInstanceListeners(),
                    properties,
                    serviceMap
            );
        }

        /**
         * Returns the configured actor context or creates one on demand.
         *
         * @return the actor context
         */
        protected ActorContext getOrCreateActorContext() {
            ActorContext actorContext = getActorContext();
            if (actorContext == null) {
                ActorContextBuilder builder = getActorContextBuilder();
                if (builder == null) {
                    builder = ActorContext.builder();
                }
                builder.withProperties(properties);
                for (Map.Entry<Class<?>, Object> entry : serviceMap.entrySet()) {
                    builder.withService((Class<Object>) entry.getKey(), entry.getValue());
                }

                return builder.createContext();
            }
            return actorContext;
        }

        /**
         * Returns the configured transaction support.
         *
         * @return the configured transaction support
         */
        public TransactionSupport getTransactionSupport() {
            return transactionSupport;
        }

        /**
         * Sets the given transaction support.
         *
         * @param transactionSupport The transaction support
         * @return this for chaining
         */
        public T withTransactionSupport(TransactionSupport transactionSupport) {
            this.transactionSupport = transactionSupport;
            return (T) this;
        }

        /**
         * Returns the configured job manager factory.
         *
         * @return the configured job manager factory
         */
        public JobManagerFactory getJobManagerFactory() {
            return jobManagerFactory;
        }

        /**
         * Sets the given job manager factory.
         *
         * @param jobManagerFactory The job manager factory
         * @return this for chaining
         */
        public T withJobManagerFactory(JobManagerFactory jobManagerFactory) {
            this.jobManagerFactory = jobManagerFactory;
            return (T) this;
        }

        /**
         * Returns the configured actor context.
         *
         * @return the configured actor context
         */
        public ActorContext getActorContext() {
            return actorContext;
        }

        /**
         * Sets the given actor context.
         *
         * @param actorContext The actor context
         * @return this for chaining
         */
        public T withActorContext(ActorContext actorContext) {
            this.actorContext = actorContext;
            return (T) this;
        }

        /**
         * Returns the configured actor context builder.
         *
         * @return the configured actor context builder
         */
        public ActorContextBuilder getActorContextBuilder() {
            return actorContextBuilder;
        }

        /**
         * Sets the given actor context builder.
         *
         * @param actorContextBuilder The actor context builder
         * @return this for chaining
         */
        public T withActorContextBuilder(ActorContextBuilder actorContextBuilder) {
            this.actorContextBuilder = actorContextBuilder;
            return (T) this;
        }

        /**
         * Returns the configured schedule factory.
         *
         * @return the configured schedule factory
         */
        public ScheduleFactory getScheduleFactory() {
            return scheduleFactory;
        }

        /**
         * Sets the given schedule factory.
         *
         * @param scheduleFactory The schedule factory
         * @return this for chaining
         */
        public T withScheduleFactory(ScheduleFactory scheduleFactory) {
            this.scheduleFactory = scheduleFactory;
            return (T) this;
        }

        /**
         * Returns the configured job processor factory.
         *
         * @return the configured job processor factory
         */
        public JobProcessorFactory getJobProcessorFactory() {
            return jobProcessorFactory;
        }

        /**
         * Sets the given job processor factory.
         *
         * @param jobProcessorFactory The job processor factory
         * @return this for chaining
         */
        public T withJobProcessorFactory(JobProcessorFactory jobProcessorFactory) {
            this.jobProcessorFactory = jobProcessorFactory;
            return (T) this;
        }

        /**
         * Returns the configured job instance processor factory.
         *
         * @return the configured job instance processor factory
         */
        public JobInstanceProcessorFactory getJobInstanceProcessorFactory() {
            return jobInstanceProcessorFactory;
        }

        /**
         * Sets the given job instance processor factory.
         *
         * @param jobInstanceProcessorFactory The job instance processor factory
         * @return this for chaining
         */
        public T withJobInstanceProcessorFactory(JobInstanceProcessorFactory jobInstanceProcessorFactory) {
            this.jobInstanceProcessorFactory = jobInstanceProcessorFactory;
            return (T) this;
        }

        /**
         * Returns the configured job scheduler factory.
         *
         * @return the configured job scheduler factory
         */
        public JobSchedulerFactory getJobSchedulerFactory() {
            return jobSchedulerFactory;
        }

        /**
         * Sets the given job scheduler factory.
         *
         * @param jobSchedulerFactory The job scheduler factory
         * @return this for chaining
         */
        public T withJobSchedulerFactory(JobSchedulerFactory jobSchedulerFactory) {
            this.jobSchedulerFactory = jobSchedulerFactory;
            return (T) this;
        }

        /**
         * Returns the configured partition keys.
         *
         * @return the configured partition keys
         */
        public Set<PartitionKey> getPartitionKeys() {
            return partitionKeys.keySet();
        }

        /**
         * Returns the configured partition key map.
         *
         * @return the configured partition key map
         */
        protected Map<PartitionKey, Integer> getPartitionKeyMap() {
            return partitionKeys;
        }

        /**
         * Adds the given partition key and sets the amount of elements that should be processed at once.
         *
         * @param partitionKey The partition key
         * @param processingCount The amount of elements to process at once for the partition
         * @return this for chaining
         */
        public T withPartitionKey(PartitionKey partitionKey, int processingCount) {
            this.partitionKeys.put(partitionKey, processingCount);
            return (T) this;
        }

        /**
         * Returns the configured partition key provider.
         *
         * @return the configured partition key provider
         */
        protected PartitionKeyProvider getPartitionKeyProvider() {
            if (partitionKeyProvider == null) {
                partitionKeyProvider = partitionKeyProviderFactory.createPartitionKeyProvider(
                        new ServiceProvider() {
                            @Override
                            public <T> T getService(Class<T> serviceClass) {
                                return serviceClass.cast(getServiceMap().get(serviceClass));
                            }
                        },
                        this::getProperty
                );
            }
            return partitionKeyProvider;
        }

        /**
         * Returns the configured partition key provider factory.
         *
         * @return the configured partition key provider factory
         */
        public PartitionKeyProviderFactory getPartitionKeyProviderFactory() {
            return partitionKeyProviderFactory;
        }

        /**
         * Sets the given partition key provider factory.
         *
         * @param partitionKeyProviderFactory The partition key provider factory
         * @return this for chaining
         */
        public T withPartitionKeyProviderFactory(PartitionKeyProviderFactory partitionKeyProviderFactory) {
            this.partitionKeyProviderFactory = partitionKeyProviderFactory;
            this.partitionKeyProvider = null;
            return (T) this;
        }

        /**
         * Returns the configured job trigger listeners.
         *
         * @return the configured job trigger listeners
         */
        public List<JobTriggerListener> getJobTriggerListeners() {
            return jobTriggerListeners;
        }

        /**
         * Adds the given job trigger listener.
         *
         * @param jobTriggerListener The job trigger listener
         * @return this for chaining
         */
        public T withJobTriggerListener(JobTriggerListener jobTriggerListener) {
            this.jobTriggerListeners.add(jobTriggerListener);
            return (T) this;
        }

        /**
         * Adds the given job trigger listeners.
         *
         * @param jobTriggerListeners The job trigger listeners
         * @return this for chaining
         */
        public T withJobTriggerListeners(List<JobTriggerListener> jobTriggerListeners) {
            this.jobTriggerListeners.addAll(jobTriggerListeners);
            return (T) this;
        }

        /**
         * Returns the configured job instance listeners.
         *
         * @return the configured job instance listeners
         */
        public List<JobInstanceListener> getJobInstanceListeners() {
            return jobInstanceListeners;
        }

        /**
         * Adds the given job instance listener.
         *
         * @param jobInstanceListener The job instance listener
         * @return this for chaining
         */
        public T withJobInstanceListener(JobInstanceListener jobInstanceListener) {
            this.jobInstanceListeners.add(jobInstanceListener);
            return (T) this;
        }

        /**
         * Adds the given job instance listeners.
         *
         * @param jobInstanceListeners The job instance listeners
         * @return this for chaining
         */
        public T withJobInstanceListeners(List<JobInstanceListener> jobInstanceListeners) {
            this.jobInstanceListeners.addAll(jobInstanceListeners);
            return (T) this;
        }

        /**
         * Returns the configured properties.
         *
         * @return the configured properties
         */
        protected Map<String, Object> getProperties() {
            return properties;
        }

        /**
         * Returns the property value for the given property key or <code>null</code>.
         *
         * @param property The property key
         * @return the property value or <code>null</code>
         */
        public Object getProperty(String property) {
            return properties.get(property);
        }

        /**
         * Sets the given property to the given value.
         *
         * @param property The property key
         * @param value The value
         * @return this for chaining
         */
        public T withProperty(String property, Object value) {
            this.properties.put(property, value);
            return (T) this;
        }

        /**
         * Adds the given properties.
         *
         * @param properties The properties
         * @return this for chaining
         */
        public T withProperties(Map<String, Object> properties) {
            this.properties.putAll(properties);
            return (T) this;
        }

        /**
         * Returns the configured service map.
         *
         * @return the configured service map
         */
        protected Map<Class<?>, Object> getServiceMap() {
            return serviceMap;
        }

        /**
         * Returns the configured services.
         *
         * @return the configured services
         */
        public Collection<Object> getServices() {
            return serviceMap.values();
        }

        /**
         * Registers the given service for the given service class.
         *
         * @param serviceClass The service class
         * @param service The service
         * @param <X> The service type
         * @return this for chaining
         */
        public <X> T withService(Class<X> serviceClass, X service) {
            this.serviceMap.put(serviceClass, service);
            return (T) this;
        }

        /**
         * A base implementation for job contexts that sub-projects can extend but can also be used for a plain job context.
         *
         * @author Christian Beikov
         * @since 1.0.0
         */
        protected static class DefaultJobContext implements JobContext {
            private static final String DEFAULT_JOB_INSTANCE_ACTOR_NAME = "jobInstanceScheduler";
            private static final int DEFAULT_JOB_INSTANCE_PROCESS_COUNT = 1;
            private static final String DEFAULT_JOB_TRIGGER_ACTOR_NAME = "jobTriggerScheduler";
            private static final int DEFAULT_JOB_TRIGGER_PROCESS_COUNT = 1;

            private final TransactionSupport transactionSupport;
            private final JobManager jobManager;
            private final ScheduleFactory scheduleFactory;
            private final JobProcessorFactory jobProcessorFactory;
            private final JobInstanceProcessorFactory jobInstanceProcessorFactory;
            private final PartitionKeyProvider partitionKeyProvider;
            private final Map<PartitionKey, JobScheduler> jobSchedulers;
            private final Map<Class<?>, List<PartitionKey>> jobInstanceClassToPartitionKeysMapping = new ConcurrentHashMap<>();
            private final JobInstanceListener[] jobInstanceListeners;
            private final Map<String, Object> properties;
            private final Map<Class<?>, Object> serviceMap;

            /**
             * Creates a job context from the given configuration.
             *
             * @param transactionSupport The transaction support
             * @param jobManagerFactory The job manager factory
             * @param actorContext The actor context
             * @param scheduleFactory The schedule factory
             * @param jobSchedulerFactory The job scheduler factory
             * @param jobProcessorFactory The job processor factory
             * @param jobInstanceProcessorFactory The job instance processor factory
             * @param partitionKeyEntries The partition key entries
             * @param partitionKeyProvider The partition key provider
             * @param jobTriggerListeners The job trigger listeners
             * @param jobInstanceListeners The job instance listeners
             * @param properties The properties
             * @param serviceMap The service map
             */
            protected DefaultJobContext(TransactionSupport transactionSupport, JobManagerFactory jobManagerFactory, ActorContext actorContext, ScheduleFactory scheduleFactory,
                                        JobSchedulerFactory jobSchedulerFactory, JobProcessorFactory jobProcessorFactory, JobInstanceProcessorFactory jobInstanceProcessorFactory,
                                        Map<PartitionKey, Integer> partitionKeyEntries, PartitionKeyProvider partitionKeyProvider, List<JobTriggerListener> jobTriggerListeners, List<JobInstanceListener> jobInstanceListeners,
                                        Map<String, Object> properties, Map<Class<?>, Object> serviceMap) {
                this.transactionSupport = transactionSupport;
                this.scheduleFactory = scheduleFactory;
                this.jobProcessorFactory = jobProcessorFactory;
                this.jobInstanceProcessorFactory = jobInstanceProcessorFactory;
                this.properties = new HashMap<>(properties);
                this.serviceMap = new HashMap<>(serviceMap);
                this.jobManager = jobManagerFactory.createJobManager(this);
                if (partitionKeyProvider == null) {
                    throw new JobException("No PartitionKeyProvider given!");
                } else {
                    this.partitionKeyProvider = partitionKeyProvider;
                }
                Collection<PartitionKey> defaultTriggerPartitionKeys = this.partitionKeyProvider.getDefaultTriggerPartitionKeys();
                if (partitionKeyEntries.isEmpty()) {
                    Collection<PartitionKey> instancePartitionKeys = this.partitionKeyProvider.getDefaultJobInstancePartitionKeys();

                    this.jobSchedulers = new HashMap<>(defaultTriggerPartitionKeys.size() + instancePartitionKeys.size());
                    for (PartitionKey instancePartitionKey : instancePartitionKeys) {
                        JobScheduler jobInstanceScheduler = jobSchedulerFactory.createJobScheduler(this, actorContext, DEFAULT_JOB_INSTANCE_ACTOR_NAME + "/" + instancePartitionKey, DEFAULT_JOB_INSTANCE_PROCESS_COUNT, instancePartitionKey);
                        jobSchedulers.put(instancePartitionKey, jobInstanceScheduler);
                    }
                } else {
                    this.jobSchedulers = new HashMap<>(defaultTriggerPartitionKeys.size() + partitionKeyEntries.size());
                    for (Map.Entry<PartitionKey, Integer> entry : partitionKeyEntries.entrySet()) {
                        jobSchedulers.put(entry.getKey(), jobSchedulerFactory.createJobScheduler(this, actorContext, DEFAULT_JOB_INSTANCE_ACTOR_NAME + "/" + entry.getKey(), entry.getValue(), entry.getKey()));
                    }
                }
                for (PartitionKey jobTriggerPartitionKey : defaultTriggerPartitionKeys) {
                    jobSchedulers.put(jobTriggerPartitionKey, jobSchedulerFactory.createJobScheduler(this, actorContext, DEFAULT_JOB_TRIGGER_ACTOR_NAME, DEFAULT_JOB_TRIGGER_PROCESS_COUNT, jobTriggerPartitionKey));
                }

                jobInstanceListeners.addAll(jobTriggerListeners);
                this.jobInstanceListeners = jobInstanceListeners.toArray(new JobInstanceListener[jobInstanceListeners.size()]);
                afterConstruct();
            }

            /**
             * Is called after finishing construction of this context object.
             */
            protected void afterConstruct() {
                start();
            }

            /**
             * Starts all job schedulers.
             */
            protected void start() {
                for (JobScheduler jobScheduler : jobSchedulers.values()) {
                    jobScheduler.start();
                }
            }

            @Override
            public Object getProperty(String property) {
                return properties.get(property);
            }

            @Override
            public <T> T getService(Class<T> serviceClass) {
                return (T) serviceMap.get(serviceClass);
            }

            @Override
            public TransactionSupport getTransactionSupport() {
                return transactionSupport;
            }

            @Override
            public JobManager getJobManager() {
                return jobManager;
            }

            @Override
            public ScheduleFactory getScheduleFactory() {
                return scheduleFactory;
            }

            @Override
            public <T extends JobTrigger> JobProcessor<T> getJobProcessor(T jobTrigger) {
                return jobProcessorFactory.createJobProcessor(this, jobTrigger);
            }

            @Override
            public <T extends JobInstance<?>> JobInstanceProcessor<?, T> getJobInstanceProcessor(T jobInstance) {
                if (jobInstance instanceof JobTrigger) {
                    return (JobInstanceProcessor<?, T>) jobProcessorFactory.createJobProcessor(this, (JobTrigger) jobInstance);
                } else {
                    return jobInstanceProcessorFactory.createJobInstanceProcessor(this, jobInstance);
                }
            }

            @Override
            public void refreshJobInstanceSchedules(JobInstance<?> jobInstance) {
                if (jobInstance.getState() != JobInstanceState.NEW) {
                    throw new JobException("JobInstance is already done and can't be scheduled: " + jobInstance);
                }
                long earliestNewSchedule = jobInstance.getScheduleTime().toEpochMilli();
                List<PartitionKey> partitionKeys = getPartitionKeys(jobInstance);
                for (int i = 0; i < partitionKeys.size(); i++) {
                    jobSchedulers.get(partitionKeys.get(i)).refreshSchedules(earliestNewSchedule);
                }
            }

            @Override
            public Collection<PartitionKey> getPartitionKeys() {
                return Collections.unmodifiableSet(jobSchedulers.keySet());
            }

            @Override
            public List<PartitionKey> getPartitionKeys(JobInstance<?> jobInstance) {
                return jobInstanceClassToPartitionKeysMapping.computeIfAbsent(jobInstance.getClass(), (k) -> {
                    List<PartitionKey> v = new ArrayList<>(jobSchedulers.keySet().size());
                    for (PartitionKey partitionKey : jobSchedulers.keySet()) {
                        if (partitionKey.getJobInstanceType().isAssignableFrom(k)) {
                            v.add(partitionKey);
                        }
                    }
                    return v;
                });
            }

            @Override
            public void refreshJobInstanceSchedules(long earliestNewSchedule) {
                for (JobScheduler jobScheduler : jobSchedulers.values()) {
                    jobScheduler.refreshSchedules(earliestNewSchedule);
                }
            }

            @Override
            public void refreshJobInstanceSchedules(PartitionKey partitionKey, long earliestNewSchedule) {
                JobScheduler jobScheduler = jobSchedulers.get(partitionKey);
                if (jobScheduler != null) {
                    jobScheduler.refreshSchedules(earliestNewSchedule);
                }
            }

            @Override
            public void forEachJobInstanceListeners(Consumer<JobInstanceListener> jobInstanceListenerConsumer) {
                for (int i = 0; i < jobInstanceListeners.length; i++) {
                    jobInstanceListenerConsumer.accept(jobInstanceListeners[i]);
                }
            }

            @Override
            public void stop() {
                for (JobScheduler jobScheduler : jobSchedulers.values()) {
                    jobScheduler.stop();
                }
            }

            @Override
            public void stop(long timeout, TimeUnit unit) throws InterruptedException {
                for (JobScheduler jobScheduler : jobSchedulers.values()) {
                    jobScheduler.stop(timeout, unit);
                }
            }
        }
    }
}

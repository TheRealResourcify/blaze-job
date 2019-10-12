[![Build Status](https://travis-ci.com/Blazebit/blaze-job.svg?branch=master)](https://travis-ci.org/Blazebit/blaze-job)

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.blazebit/blaze-job-core-api/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.blazebit/blaze-job-core-api)
[![Slack Status](https://blazebit.herokuapp.com/badge.svg)](https://blazebit.herokuapp.com)

[![Javadoc - Job](https://www.javadoc.io/badge/com.blazebit/blaze-job-core-api.svg?label=javadoc%20-%20job-api)](http://www.javadoc.io/doc/com.blazebit/blaze-job-core-api)

Blaze-Job
==========
Blaze-Job is an extendible toolkit for job scheduling with a strong focus on allowing efficient persistent job pipelines in a cluster environment.

What is it?
===========

Blaze-Job provides a lightweight job execution runtime built on top of [Blaze-Actor](https://github.com/Blazebit/blaze-actor) that can make use of a pluggable job storage.
 
The job scheduler is partition and cluster aware and has the ability to reschedule jobs when temporary errors occur.
There are two base implementations for job storages, an in-memory implementation and a JPA implementation that can be used for custom storage models.
Job triggers can schedule jobs through a cron expression. Job instances support incremental or at once processing.

In short, Blaze-Job is a runtime that can be used with a custom job model that supports partitioning in a cluster environment.

Features
==============

Blaze-Job has support for

* Pluggable job model and job storage
* Base implementations for in-memory or JPA based job storage
* Recurring jobs through job triggers
* Incremental processing of job instances
* Cluster and partitioning support in the job scheduler

How to use it?
==============

Blaze-Job is split up into different modules. We recommend that you define a version property in your parent pom that you can use for all artifacts. Modules are all released in one batch so you can safely increment just that property. 

```xml
<properties>
    <blaze-job.version>1.0.0-SNAPSHOT</blaze-job.version>
    <blaze-actor.version>1.0.0-Alpha1</blaze-actor.version>
</properties>
```

Alternatively you can also use our BOM in the `dependencyManagement` section.

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.blazebit</groupId>
            <artifactId>blaze-job-bom</artifactId>
            <version>1.0.0-SNAPSHOT</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>    
    </dependencies>
</dependencyManagement>
```

## Manual setup

For compiling you will only need API artifacts and for the runtime you need impl and integration artifacts.

Blaze-Job Core module dependencies

```xml
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-core-api</artifactId>
    <version>${blaze-job.version}</version>
    <scope>compile</scope>
</dependency>
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-core-impl</artifactId>
    <version>${blaze-job.version}</version>
    <scope>runtime</scope>
</dependency>
```

Blaze-Job JPA module dependencies

```xml
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-jpa-model</artifactId>
    <version>${blaze-job.version}</version>
    <scope>compile</scope>
</dependency>
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-jpa-storage</artifactId>
    <version>${blaze-job.version}</version>
    <scope>runtime</scope>
</dependency>
```

Blaze-Job Memory module dependencies

```xml
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-memory-model</artifactId>
    <version>${blaze-job.version}</version>
    <scope>compile</scope>
</dependency>
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-memory-storage</artifactId>
    <version>${blaze-job.version}</version>
    <scope>runtime</scope>
</dependency>
```

Blaze-Job scheduler implementation for Blaze-Actor. Use either of the two, the Spring module if you are on Spring

```xml
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-actor-scheduler-executor</artifactId>
    <version>${blaze-actor.version}</version>
    <scope>compile</scope>
</dependency>
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-actor-scheduler-spring</artifactId>
    <version>${blaze-actor.version}</version>
    <scope>compile</scope>
</dependency>
```

Blaze-Job Schedule support. Use either of the two, the Spring module if you are on Spring

```xml
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-schedule-cron</artifactId>
    <version>${blaze-job.version}</version>
    <scope>runtime</scope>
</dependency>
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-schedule-spring</artifactId>
    <version>${blaze-job.version}</version>
    <scope>runtime</scope>
</dependency>
```

Blaze-Job Transaction support. Use either of the three, depending on the transaction API of the target environment

```xml
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-transaction-jpa</artifactId>
    <version>${blaze-job.version}</version>
    <scope>runtime</scope>
</dependency>
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-transaction-jta</artifactId>
    <version>${blaze-job.version}</version>
    <scope>runtime</scope>
</dependency>
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-transaction-spring</artifactId>
    <version>${blaze-job.version}</version>
    <scope>runtime</scope>
</dependency>
```

Blaze-Job Processor base implementations

```xml
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-processor-hibernate-insert-select</artifactId>
    <version>${blaze-job.version}</version>
    <scope>compile</scope>
</dependency>
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-processor-memory</artifactId>
    <version>${blaze-job.version}</version>
    <scope>compile</scope>
</dependency>
```

Documentation
=========

Currently there is no documentation other than the Javadoc.
 
Quick-start
=================

To work with Blaze-Job, one has to first setup a custom job model. For illustration purposes, we use the memory storage.  

```java
public class MyJobInstance extends AbstractJobInstance<Long> {

    private JobConfiguration jobConfiguration = new JobConfiguration();

    public MyJobInstance() {
        setCreationTime(Instant.now());
    }

    @Override
    public Long getPartitionKey() {
        return getId();
    }

    @Override
    public JobConfiguration getJobConfiguration() {
        return jobConfiguration;
    }

    @Override
    public void onChunkSuccess(JobInstanceProcessingContext<?> processingContext) {
    }
}
```

With that class in place, we can define a job context.

```java
JobContext jobContext = JobContext.builder()
    // Don't support job triggers here
    .withJobProcessorFactory(JobProcessorFactory.of(((jobTrigger, context) -> {})))
    // 
    .withJobInstanceProcessorFactory(JobInstanceProcessorFactory.of(((jobInstance, context) -> {
        return (jobInstance, context) -> {
            System.out.println("Hello from job processor for: " + jobInstance);
            return null;
        };
    })))
    .withProperty(ExecutorServiceScheduler.EXECUTOR_SERVICE_PROPERTY, Executors.newScheduledThreadPool(2))
    .createContext();
```

With this job context, every job instance that is scheduled will print to the console.
To schedule a job instance, the job instance has to be configured and added to the job manager.

```java
MyJobInstance jobInstance = new MyJobInstance();
jobInstance.setScheduleTime(Instant.now());
jobContext.getJobManager().addJobInstance(jobInstance);
```

The job is scheduled, executed and then marked as done.

Licensing
=========

This distribution, as a whole, is licensed under the terms of the Apache
License, Version 2.0 (see LICENSE.txt).

References
==========

Project Site:              https://job.blazebit.com (coming at some point)

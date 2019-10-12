[![Build Status](https://travis-ci.com/Blazebit/blaze-job.svg?branch=master)](https://travis-ci.org/Blazebit/blaze-job)

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.blazebit/blaze-job-core-api/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.blazebit/blaze-job-core-api)
[![Slack Status](https://blazebit.herokuapp.com/badge.svg)](https://blazebit.herokuapp.com)

[![Javadoc - Job](https://www.javadoc.io/badge/com.blazebit/blaze-job-core-api.svg?label=javadoc%20-%20job-api)](http://www.javadoc.io/doc/com.blazebit/blaze-job-core-api)

Blaze-Job
==========
Blaze-Job is a toolkit that can be used to build a custom job and predicate DSL based on blaze-domain with a strong focus on string and query serialization of jobs.

What is it?
===========

Blaze-Job provides a common job AST that is enriched with blaze-domain types which enables string and query serialization. 

The job AST in the core API module is supposed to be general purpose and supposed to cover mostly structural aspects.
An job compiler that implements a syntax similar to the JPQL.Next job language is provided out of the box, but a custom syntax can be used by providing a custom compiler.
The implementation provides support for an interpreter for jobs, given that the domain types have proper interpreters registered for the operations registered. 
Blaze-Job comes with a persistence module that provides an interpreter and JPQL.Next rendering support to a string or Blaze-Persistence query builders for persistence related models.
The _declarative_ submodule allows to define job related metadata via annotations on the domain model elements.

In short, Blaze-Job allows you to have a custom DSL based on your own domain model that translates into queries for efficient database execution but also supports interpretation and serialization for storage.

Features
==============

Blaze-Job has support for

* Make use of custom domain model in DSL via Blaze-Domain
* Serialize jobs to string form for storage
* Serialize jobs to jobs/predicates in Blaze-Persistence queries
* Interpret jobs on custom objects
* Full custom function support

How to use it?
==============

Blaze-Job is split up into different modules. We recommend that you define a version property in your parent pom that you can use for all artifacts. Modules are all released in one batch so you can safely increment just that property. 

```xml
<properties>
    <blaze-job.version>1.0.0-SNAPSHOT</blaze-job.version>
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

Blaze-Job Declarative module dependencies

```xml
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-declarative-api</artifactId>
    <version>${blaze-job.version}</version>
    <scope>compile</scope>
</dependency>
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-declarative-impl</artifactId>
    <version>${blaze-job.version}</version>
    <scope>runtime</scope>
</dependency>
```

Blaze-Job Persistence module dependencies

```xml
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-persistence</artifactId>
    <version>${blaze-job.version}</version>
    <scope>compile</scope>
</dependency>
<dependency>
    <groupId>com.blazebit</groupId>
    <artifactId>blaze-job-declarative-persistence</artifactId>
    <version>${blaze-job.version}</version>
    <scope>compile</scope>
</dependency>
```

Documentation
=========

Currently there is no documentation other than the Javadoc.
 
Core quick-start
=================

To work with Blaze-Job, a `JobServiceFactory` is needed which requires that a domain model is built first.  

```java
DomainBuilder domainBuilder = Domain.getDefaultProvider().createDefaultBuilder();
domainBuilder.createEntityType("Cat")
    .addAttribute("name", String.class)
    .addAttribute("age", Integer.class)
  .build();
DomainModel domain = domainBuilder.build();
```

With that `DomainModel` a `JobServiceFactory` can be created and an job compiled.

```java
JobServiceFactory jobServiceFactory = Jobs.forModel(domain);
JobCompiler compiler = jobServiceFactory.createCompiler();
JobCompiler.Context context = compiler.createContext(Collections.singletonMap("c", domain.getType("Cat")));
Job job = compiler.createJob("c.age", context);
```

The job string is parsed, type checked and enriched with the result domain types.
The metadata defined for domain types is then used internally to implement interpretation or serialization.

Such a simple job isn't very interesting, but to go further, the definition of some basic types and their operators is necessary which is provided by the persistence module.

Declarative Persistence usage
=================

The persistence and declarative persistence modules allow to make use of some commonly used basic types and provide a wide set of builtin functions:

```java
@DomainType
@EntityType(Cat.class)
interface CatModel {
  @EntityAttribute
  String getName();
  @EntityAttribute("AGE(birthday)")
  Integer getAge();
}
```

Assuming the domain model was already built, we could formulate a predicate:

```java
JobServiceFactory jobServiceFactory = Jobs.forModel(domain);
JobCompiler compiler = jobServiceFactory.createCompiler();
JobCompiler.Context context = compiler.createContext(
    Collections.singletonMap("c", domain.getType("CatModel"))
);
Predicate predicate = compiler.createPredicate("c.age > 18", context);
```

The predicate could be evaluated against an object i.e. interpreted

```java
JobInterpreter interpreter = jobServiceFactory.createInterpreter();
JobInterpreter.Context context = interpreter.createContext(
    Collections.singletonMap("c", domain.getType("CatModel")),
    Collections.singletonMap("c", new CatModelImpl("Cat 1", 19))
);
Boolean result = interpreter.evaluate(predicate, context);
```

This would yield `true` as the age of the cat in the example is 19. This could also be serialized to a query

```java
CriteriaBuilder<Cat> criteriaBuilder = criteriaBuilderFactory.create(entityManager, Cat.class, "cat");
JobSerializer<WhereBuilder> serializer = jobServiceFactory.createSerializer(WhereBuilder.class);
JobSerializer.Context context = serializer.createContext(
    Collections.singletonMap("c", "cat")
);
serializer.serializeTo(context, predicate, criteriaBuilder);
```

This will result in a query like the following

```sql
SELECT cat
FROM Cat cat
WHERE AGE(cat.birthday) > 18
```

Licensing
=========

This distribution, as a whole, is licensed under the terms of the Apache
License, Version 2.0 (see LICENSE.txt).

References
==========

Project Site:              https://job.blazebit.com (coming at some point)

# Spring Boot Batch-Integration Sample
Sample project for Spring Batch and Spring Integration using Spring Boot and Java Config

## Goal
Build a processing flow using the IntegrationFlow configuration builder and general Java Config instead of xml bean configuration to activate batch jobs and read/write message channels.

## Spring Integration + Spring Batch + JPA
In this sample we have a integration flow where we pool files in a specific directory and request batch jobs to process those files persisting the in a memory database using JPA repositories.

### Usage
Just run it, spring boot maven plugin do all the dirty job for you:

`$ mvn spring-boot:run` 

### References

* https://github.com/spring-projects/spring-integration-java-dsl
* https://github.com/mminella/SpringBatchWebinar
* https://github.com/xpadro/spring-integration
* https://github.com/ghillert/spring-batch-integration-sample

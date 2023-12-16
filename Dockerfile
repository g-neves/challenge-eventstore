FROM maven:3.8.4-openjdk-11 AS builder

WORKDIR /usr/src/app

COPY src/ ./src
COPY pom.xml ./

RUN mvn clean test



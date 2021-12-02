#!/bin/bash
# Clean and remove old build artifacts
mvn clean -f pom.xml
# Rebuild the project jar file
mvn verify -f pom.xml
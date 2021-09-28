FROM java:8
EXPOSE 8080
ADD /target/springboot-demo-0.0.1-SNAPSHOT.jar springboot-demo.jar
ENTRYPOINT ["java","-jar","springboot-demo.jar"]
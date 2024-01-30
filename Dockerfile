FROM bitnami/spark:3.5

# docker.io/bitnami/spark:3.5

# Install additional Python dependencies if needed
RUN pip install pyspark fastapi uvicorn

# USER 1001
# RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.704/aws-java-sdk-bundle-1.11.704.jar --output aws-java-sdk-bundle-1.11.704.jar
# RUN curl https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.25/redshift-jdbc42-2.1.0.25.jar --output redshift-jdbc42-2.1.0.25.jar

#RUN apt-get install curl
COPY aws-java-sdk-bundle-1.11.704.jar /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.704.jar

COPY redshift-jdbc42-2.1.0.25.jar /opt/bitnami/spark/jars/redshift-jdbc42-2.1.0.25.jar

# COPY spark-redshift_2.10-2.0.0.jar /opt/bitnami/spark/jars/spark-redshift_2.10-2.0.0.jar

# Solr Similarity
This project provides offers Spark based solution for computing similarity between documents in solr index.


## Requirements
+ Maven 3.x
+ JDK 1.8.x
+ Spark (Fetched from maven repo)


## Build :
+ to compile : `mvn clean compile`
+ to build : `mvn clean package`

## to run :
+ Usage
```
$ java -jar target/solr-similarity-1.0-SNAPSHOT.jar
     -master VAL          : spark master name or url (default: local)
     -measure [cosine]    : name of similarity measure to use (default: cosine)
     -out FILE            : path to directory for storing the output
     -query VAL           : Solr query for selecting the documents (default: *:*)
     -solr URL            : Solr URL
     -vectorizer [simple] : name of vectorizer to use (default: simple)
```

+ Example to find similarity between documents matching to query "flash"

```
 java -jar target/solr-similarity-1.0-SNAPSHOT.jar \
    -solr http://localhost:8983/solr/collection1 \
    -query "flash" \
    -out cos-similarity.out

```

# DDR-BM25
Distributed Doc Retrieval with Scala & Spark - Scalable and Cloud Programming project a.y. 2020/2021

## Description
DDR-BM25 is a distributed document retrieval based on OkapiBM25. The program matches a user query against a set of unstructured texts and shows the 10 most relevant documents for that query. 

## Environment
- **scala** v. 2.12.11
- **spark** v. 3.0.1
- **sbt** v. 1.4.3
- **AWS**
  - **EMR** v. emr-6.2.0 with Spark 3.0.1 and Zeppelin 0.9.0
  - **S3** 


## Database
One or more JSON file with the following format: 
```json
{
   "mediawiki":{
      "page":[
         {
            "id":4776,
            "title":"Document Title",
            "revision":{
               "text":{
                  "content":"the document text"
               }
            }
         },
         {
            "id":4222,
            "title":"Document Title2",
            "revision":{
               "text":{
                  "content":"the document text"
               }
            }
         }
      ],
      "version":0.1
   }
}
```
Json files can have any name. 

## 1. Create the Jar File 
You need to create a JAR file that you will upload on your S3 bucket. 
To create the JAR, run the following command: 

``` sbt package ```

It will generate a jar file named __scp-project_2.12-0.3.jar__ inside the directory _target/scala-2.12/_

## 2. Set up and run 
1. Create a S3 bucket.
2. Upload the jar file inside your S3 bucket.
3. Create a directory named _input_ at the root of your bucket.
4. Upload your json files in the _input_  directory. 
5. Connect to an EMR master instance of your cluster via SSH. 
6. Execute jar file using ```spark-submit``` command.

## Spark-submit arguments 
The program can execute the following commands:
- **-f** : bucket path.
- **-c** : non-negative number of partition
- **-p** : preprocess '''true/false'''. Note: it must be '''true''' the first time you run the program. 
- **-q** : input query. The stirng must be inside the double quotes ""

For example: 
```spark-submit \
-v \
--deploy-mode cluster \
scp-project_2.12-0.3.jar -f="s3://bucketname/" -c=112 -p=true -q="Victorian era"```


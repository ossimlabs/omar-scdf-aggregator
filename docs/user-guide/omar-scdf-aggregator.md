# omar-scdf-aggregator
The Aggregator is a Spring Cloud Data Flow (SCDF) Processor.
This means it:
1. Receives a message on a Spring Cloud input stream using Kafka.
2. Performs an operation on the data.
3. Sends the result on a Spring Cloud output stream using Kafka to a listening SCDF Processor or SCDF Sink.

## Purpose
The Aggregator receives a JSON message from the S3 Filter containing an AWS S3 bucket name and the name of a zip file/object in the bucket. The aggregator then creates a list of all the objects related to that zip file, such as .email files, and generates a JSON list of files for the Downloader.

## JSON Input Example (from the S3 Filter)
```json
{
   "bucket":"omar-dropbox",
   "filename":"12345/SCDFTestImages.zip"
}
```

## JSON Output Example (to the Downloader)
```json
{
   "files":[
      {
         "bucket":"omar-dropbox",
         "filename":"12345/SCDFTestImages.zip"
      },
      {
         "bucket":"omar-dropbox",
         "filename":"12345/SCDFTestImages.zip_56734.email"
      }
   ]
}
```

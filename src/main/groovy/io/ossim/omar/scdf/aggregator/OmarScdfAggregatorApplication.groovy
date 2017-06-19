package io.ossim.omar.scdf.aggregator

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.ObjectListing
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.beans.factory.annotation.Autowired
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j

/**
 * Created by adrake on 5/31/2017
 * Updated by slallier on 6/20/2017
 */
@SpringBootApplication
@EnableBinding(Processor.class)
@Slf4j
class OmarScdfAggregatorApplication {
	/**
	 * File extension passed in from application.properties
	 */
	@Value('${fileExtension1:.zip}')
	String fileExtension1

    /**
     * File extension passed in from application.properties
     */
    @Value('${fileExtension2:.email}')
    String fileExtension2

	/**
	 * S3 Client
	 */
	@Autowired
	private AmazonS3Client s3Client

	/**
	 * The main entry point of the SCDF Aggregator application.
	 * @param args
	 */
	static final void main(String[] args)
    {
		SpringApplication.run OmarScdfAggregatorApplication, args
	}

	/**
	 * Receives a message from a SCDF SQS Notifier.  Checks the given S3 bucket
	 * for the configured files, and drops the message, or send an aggregate message
	 * to the next SCDF application in the chain.
	 *
	 * @param message The message object the SQS Notifier (in JSON)
	 * @return a JSON message of the files, and bucket that need to be downloaded
	 */
	@StreamListener(Processor.INPUT)
    @SendTo(Processor.OUTPUT)
	final String aggregate(final Message<?> message)
    {
        log.debug("Message received: ${message}")

        if (null != message.payload)
        {
            // Parse the message
            final def parsedJson = new JsonSlurper().parseText(message.payload)
            final String bucketName = parsedJson.bucket
            final String fileFromJson = parsedJson.filename
            final String fileNameFromMessage = fileFromJson[0..fileFromJson.lastIndexOf('.') - 1]
            final String fileExtensionFromMessage = fileFromJson[fileFromJson.lastIndexOf('.')..fileFromJson.length() - 1]
            final String directoryPath = fileFromJson[0..fileFromJson.lastIndexOf('/') - 1]

            boolean emailFileExists = false
            JsonBuilder filesToDownload

            log.debug("parsedJson : ${parsedJson}")
            log.debug("bucketName:  ${bucketName}")
            log.debug("fileFromJson: ${fileFromJson}")
            log.debug("\n-- Parsed Message --\nfileName: ${fileNameFromMessage} \nfileExtension: ${fileExtensionFromMessage}\nbucketName: ${bucketName}\n")

            if (fileExtension1 == fileExtensionFromMessage)
            {
                log.debug("fileExtension1 matches file extension from message, building JSON with aggregated files")

                final List<BucketFile> listOfBucketfiles = new ArrayList<>()

                final ListObjectsRequest lor = new ListObjectsRequest()
                        .withBucketName(bucketName)
                        .withPrefix(directoryPath)
                final ObjectListing objectListing = s3Client.listObjects(lor)

                for (S3ObjectSummary summary : objectListing.getObjectSummaries())
                {
                    final def file = new BucketFile(summary.bucketName, summary.key)

                    if (!summary.key.endsWith('/'))
                    {
                        listOfBucketfiles.add(file)
                    }

                    if (summary.key.endsWith(fileExtension2))
                    {
                        emailFileExists = true
                    }
                }

                filesToDownload = new JsonBuilder()
                filesToDownload(files: listOfBucketfiles)
            }

            if (emailFileExists)
            {
                log.debug("filesToDownload: ${filesToDownload}")
                return filesToDownload.toString()

            }
            else
            {
                log.warn("No associated email file found, returning null!")
                return null
            }
        }
        else
        {
            log.warn("Received null mesage!")
            return null
        }
	}

    /**
     * Private container class for files in S3
     */
    private class BucketFile
    {
        String bucket
        String filename

        BucketFile(String bucketName, String filename){
            this.bucket = bucketName
            this.filename = filename
        }
    }
}

package io.ossim.omar.scdf.aggregator

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.core.io.support.ResourcePatternResolver
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.io.ResourceLoader
import org.springframework.core.io.Resource
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Created by adrake on 5/31/2017
 */

@SpringBootApplication
@EnableBinding(Processor.class)
class OmarScdfAggregatorApplication {

	/**
	 * The application logger
	 */
	private final Logger logger = LoggerFactory.getLogger(this.getClass())

	/**
	 * File extension passed in from application.properties
	 */
	@Value('${fileExtension1}')
	String fileExtension1

	/**
	 * File extension passed in from application.properties
	 */
	@Value('${fileExtension2}')
	String fileExtension2

	/**
	 * ResouceLoader used to access the s3 bucket objects
	 */
	@Autowired
	private ResourceLoader resourceLoader

	/**
	 * Provides a URI for the s3
	 */
	@Autowired
	private ResourcePatternResolver resourcePatternResolver

    OmarScdfAggregatorApplication() {
        if(null == fileExtension1){
            fileExtension1 = ".zip"
        }

        if(null == fileExtension2){
            fileExtension2 = ".txt"
        }
    }

	/**
	 * The main entry point of the SCDF Aggregator application.
	 * @param args
	 */
	static final void main(String[] args) {
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
	@StreamListener(Processor.INPUT) @SendTo(Processor.OUTPUT)
	final String aggregate(final Message<?> message){

		if(logger.isDebugEnabled()){
			logger.debug("Message received: ${message}")
		}

        JsonBuilder filesToDownload

        logger.debug("Message payload: ${message.payload}")
        if(null != message.payload) {

            // Parse the message
            final def parsedJson = new JsonSlurper().parseText(message.payload)
            logger.debug("parsedJson : ${parsedJson}")
            logger.debug("parsedJson.bucket : ${parsedJson.bucket}")
            logger.debug("parsedJson.bucket : ${parsedJson.filename}")
            final String bucketName = parsedJson.file.bucket
            logger.debug("bucketName:  ${bucketName}")
            final String fileFromJson = parsedJson.file.filename
            logger.debug("fileFromJson: ${fileFromJson}")
            final String fileNameFromMessage = fileFromJson[0..fileFromJson.lastIndexOf('.') - 1]
            logger.debug("fileNameFromMessage: ${fileNameFromMessage}")
            final String fileExtensionFromMessage = fileFromJson[fileFromJson.lastIndexOf('.')..fileFromJson.length() - 1]
            logger.debug("fileExtensionFromMessage: ${fileExtensionFromMessage}")

            if (logger.isDebugEnabled()) {
                logger.debug("\n-- Parsed Message --\nfileName: ${fileNameFromMessage} \nfileExtension: ${fileExtensionFromMessage}\nbucketName: ${bucketName}\n")
            }

            // TODO:
            // This assumes we will always be looking for two files with the aggregator.  Should
            // we make it so that we can also look for one, or maybe three???
            if (fileExtension1 == fileExtensionFromMessage) {

                if (logger.isDebugEnabled()) {
                    logger.debug("fileExtension1 matches file extension from message")
                }

                // Looks for the associated file.  Example: .txt
                final String fileToLookFor = "${fileNameFromMessage}${fileExtension2}"

                final String s3Uri = "s3://${bucketName}/${fileToLookFor}"

                final Resource s3FileResource = this.resourcePatternResolver.getResource(s3Uri)

                if (s3FileResource.exists()) {
                    // The other file exists! Put both files in a JSON array to send to next processor

                    // TODO make this dynamic for N files to download
                    final def file1 = new BucketFile(bucketName, "${fileNameFromMessage}${fileExtension1}")
                    final def file2 = new BucketFile(bucketName, "${fileNameFromMessage}${fileExtension2}")
                    final def fileList = [file1, file2]

                    filesToDownload = new JsonBuilder()
                    filesToDownload(files: fileList)

                } else {
                    logger.warn("""
					Received notification for file that does not exist:
					${s3FileResource.filename}
					""")
                }
            }

            if (logger.isDebugEnabled()) {
                logger.debug("filesToDownload: ${filesToDownload}")
            }
        }
		return filesToDownload.toString()
	}

    /**
     * Private container class for files in S3
     */
    private class BucketFile{
        String bucket
        String filename

        BucketFile(String aBucket, String aFilename){
            this.bucket = aBucket
            this.filename = aFilename
        }
    }
}

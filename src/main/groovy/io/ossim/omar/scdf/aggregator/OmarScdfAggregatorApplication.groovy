package io.ossim.omar.scdf.aggregator

import groovy.json.JsonOutput
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
	private Logger logger = LoggerFactory.getLogger(this.getClass())

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

	/**
	 * The main entry point of the SCDF Aggregator application.
	 * @param args
	 */
	static void main(String[] args) {
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
	String transform(Message<?> message){

		if(logger.isDebugEnabled()){
			logger.debug("Message received: ${message}")
		}

        JsonBuilder filesToDownload

        // Parse the message
		def parsedJson = new JsonSlurper().parseText(message.payload)
		def bucketName = parsedJson.file.bucket[0]
		def fileFromJson = parsedJson.file.filename[0]
		def fileNameFromMessage = fileFromJson[0..fileFromJson.lastIndexOf('.') - 1]
		def fileExtensionFromMessage = fileFromJson[fileFromJson.lastIndexOf('.')..fileFromJson.length() - 1]

		if(logger.isDebugEnabled()){
			logger.debug("\n-- Parsed Message --\nfileName: ${fileNameFromMessage} \nfileExtension: ${fileExtensionFromMessage}\nbucketName: ${bucketName}\n")
		}

		// TODO:
		// This assumes we will always be looking for two files with the aggregator.  Should
		// we make it so that we can also look for one, or maybe three???
		if (fileExtension1 == fileExtensionFromMessage) {

            if(logger.isDebugEnabled()){
                logger.debug("fileExtension1 matches file extension from message")
            }

			// Looks for the associated file.  Example: .txt
            def fileToLookFor = "${fileNameFromMessage}${fileExtension2}"

			def s3Uri = "s3://${bucketName}/${fileToLookFor}"

			Resource s3FileResource = this.resourcePatternResolver.getResource(s3Uri)

			if(s3FileResource.exists()){
                // The other file exists! Put both files in a JSON array to send to next processor

                // TODO make this dynamic for N files to download
                def file1 = new BucketFile(bucket: bucketName, filename: "${fileNameFromMessage}${fileExtension1}")
                def file2 = new BucketFile(bucket: bucketName, filename: "${fileNameFromMessage}${fileExtension2}")
                def fileList = [file1, file2]

                filesToDownload = new JsonBuilder()
                filesToDownload(files: fileList)

			} else {
				logger.warn("""
					Received notification for file that does not exist:
					${s3FileResource.filename}
					""")
			}
		}

		if(logger.isDebugEnabled()){
			logger.debug("filesToDownload: ${filesToDownload}")
		}
		return filesToDownload.toString()
	}

    private class BucketFile{
        def bucket
        def filename
    }
}
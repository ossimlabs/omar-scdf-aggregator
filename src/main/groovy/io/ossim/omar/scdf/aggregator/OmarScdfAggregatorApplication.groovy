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

@SpringBootApplication
@EnableBinding(Processor.class)
class OmarScdfAggregatorApplication {



	static void main(String[] args) {
		SpringApplication.run OmarScdfAggregatorApplication, args
	}

	@Value('${testThingy}')
	String testThingy

	@Value('${fileExtension1}')
	String fileExtension1

	@Value('${fileExtension2}')
	String fileExtension2

	@Autowired
	private ResourceLoader resourceLoader

	@Autowired
	private ResourcePatternResolver resourcePatternResolver

	@StreamListener(Processor.INPUT) @SendTo(Processor.OUTPUT)
	String transform(Message<?> message){

		println message

		def filesToDownload
		def payload = new JsonSlurper().parseText(message.payload)
		def bucketName = payload.bucket

		def fileName = payload.filename[0..payload.filename.lastIndexOf('.') - 1]
		def fileExtension = payload.filename[payload.filename.lastIndexOf('.')..payload.filename.length() - 1]
		def fileToLookFor = ""

		println "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
		println "@@@@@@@@@@@@@@@@@@@@@@@ ${fileExtension2} @@@@@@@@@@@@@@@@@@@@@@@@@@@"
		println "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"

		// TODO: Filter file extensions via config

		// This assumes we will always be looking for two files with the aggregator.  Should
		// we make it so that we can also look for one, or maybe three???
		//if (".zip".equals(fileExtension)) {

		if (fileExtension1 == fileExtension) {

			//fileToLookFor = "${fileName}.txt"

			// Looks for the associated file.  Example: .txt
			fileToLookFor = "${fileName}${fileExtension2}"

			def s3 = "s3://${bucketName}/${fileToLookFor}"

			try {

				Resource fileFoundInS3 = this.resourcePatternResolver.getResource(s3)

				if(fileFoundInS3.exists()){

					// The other file exists! Put both files in a JSON array to send to next processor
					def file = new BucketFile()
					//file.filename = "${fileName}.zip"
					file.filename = "${fileName}${fileExtension1}"
					file.bucket = bucketName

					def file2 = new BucketFile()
					//file.filename = "${fileName}.txt"
					file2.filename = "${fileName}${fileExtension2}"
					file2.bucket = bucketName

					filesToDownload = JsonOutput.toJson([file, file2])
					println(filesToDownload)
				}

			}
			catch (Exception ex) {

				// No file found in S3, die
				println ex.printStackTrace()

			}
		}

		return filesToDownload

	}

	private final class BucketFile {
		String filename
		String bucket
	}
}
package io.ossim.omar.scdf.aggregator

import groovy.json.JsonOutput
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.core.io.support.ResourcePatternResolver
import org.springframework.integration.annotation.Filter
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

	@Autowired
	private ResourceLoader resourceLoader

	@Autowired
	private ResourcePatternResolver resourcePatternResolver

	@Filter(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
	public boolean filter(Message<?> message) {

		def filesToDownload
		def payload = new JsonSlurper().parseText(message.payload)
		def bucketName = payload.bucket

		def fileName = payload.filename[0..payload.filename.lastIndexOf('.') - 1]
		def fileExtension = payload.filename[payload.filename.lastIndexOf('.')..payload.filename.length() - 1]
		def fileToLookFor = ""

		// TODO: Filter file extensions via config
		if (".zip".equals(fileExtension)) {
			fileToLookFor = "${fileName}.txt"
		} else if (".txt".equals(fileExtension)) {
			fileToLookFor = "${fileName}.zip"
		}
		// else wtf kind of file?

		def s3 = "s3://${bucketName}/${fileToLookFor}"

		try {

			Resource fileFoundInS3 = this.resourcePatternResolver.getResource(s3)
			println(fileFoundInS3)

			// The other file exists! Put both files in a JSON array to send to next processor
			def zipFile = new BucketFile()
			zipFile.filename = "${fileName}.zip"
			zipFile.bucket = bucketName
			def txtFile = new BucketFile()
			txtFile.filename = "${fileName}.txt"
			txtFile.bucket = bucketName

			filesToDownload = JsonOutput.toJson([zipFile, txtFile])
			println(filesToDownload)
		}
		catch (Exception ex) {

			// No file found in S3, die
			println ex.printStackTrace()

		}

		return filesToDownload != null

	}

	private final class BucketFile {
		String filename;
		String bucket;
	}
}
package io.ossim.omar.scdf.aggregator

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.core.io.support.ResourcePatternResolver
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.io.ResourceLoader
import org.springframework.core.io.Resource


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

	@StreamListener(Processor.INPUT) @SendTo(Processor.OUTPUT)
	String transform(ReceivedAwsData receivedAwsData){

		try {

			Resource[] allZipFilesInFolder = this.resourcePatternResolver.getResources("s3://omar-dropbox/*.zip")
			String outputMessage

			allZipFilesInFolder.each {

				println "Incoming SQS filename: ${receivedAwsData.filename}"
				println 'Checking against Bucket filename: ' + it.filename

				def f = it.filename
				println f

				if(f == receivedAwsData.filename) {

					println "##################"
					println "Bingo!!! ${f} is here in the bucket!!!! "
					println "##################"

					outputMessage = receivedAwsData.filename + " " + receivedAwsData.bucket
				}
				else {

					outputMessage = "File not found yet!"

				}

			}

			return outputMessage

		}
		catch(Exception ex){

			println "Exception ${ex}"

		}

		//return receivedAwsData.filename + " " + receivedAwsData.bucket


	}

}

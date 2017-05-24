package io.ossim.omar.scdf.aggregator

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.messaging.handler.annotation.SendTo


@SpringBootApplication
@EnableBinding(Processor.class)
class OmarScdfAggregatorApplication {

	static void main(String[] args) {
		SpringApplication.run OmarScdfAggregatorApplication, args
	}

	@StreamListener(Processor.INPUT) @SendTo(Processor.OUTPUT)
	String transform(ReceivedAwsData receivedAwsData){


		//println receivedAwsData

		// TODO: When the filename comes through we need to check the s3
		//		 bucket to see if it is in the bucket.
		//		 File types:
		//		 1. foo.txt
		//		 1. foo.zip





		if(receivedAwsData.filename == 'foo.zip') {
			println 'Yep, it is foo.zip'
		}
		else {
			println 'Nope, it is NOT foo.tif'
		}









		return receivedAwsData.filename + " " + receivedAwsData.bucket


	}

}

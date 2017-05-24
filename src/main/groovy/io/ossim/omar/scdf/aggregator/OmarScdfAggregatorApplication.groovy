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
		println receivedAwsData
		return receivedAwsData.filename + " " + receivedAwsData.bucket
	}

}

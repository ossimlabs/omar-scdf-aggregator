package io.ossim.omar.scdf.aggregator
/**
 * Created by adrake on 5/24/17.
 */
import org.springframework.boot.context.properties.ConfigurationProperties;
import groovy.transform.ToString;

@ToString(includeNames = true)
@ConfigurationProperties(prefix = "cloud.aws.credentials")
class AwsCredentialsProperties {

    String accessKey;

    String secretKey;

}

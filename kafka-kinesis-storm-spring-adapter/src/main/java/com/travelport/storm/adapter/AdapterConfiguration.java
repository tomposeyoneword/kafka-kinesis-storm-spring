package com.travelport.storm.adapter;

//@Configuration
public class AdapterConfiguration {

    //    @Value("${aws.access.key:access_key_id_123}")
    //    String accessKey;
    //
    //    @Value("${aws.access.key:secret_key_id_123}")
    //    String secretKey;
    //
    //    @Value("${aws.producer.config.file}")
    //    private String kinesisProducerConfigFile;
    //
    //    @Bean
    //    public KinesisProducer kinesisProducer() throws FileNotFoundException {
    //        String kinesisProducerConfigFilePath = ResourceUtils.getFile(kinesisProducerConfigFile).getPath();
    //        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
    //        AWSStaticCredentialsProvider awsCredentialProvider = new AWSStaticCredentialsProvider(awsCredentials);
    //        KinesisProducerConfiguration kinesisProducerConfig = KinesisProducerConfiguration
    //                .fromPropertiesFile(kinesisProducerConfigFilePath)
    //                .setCredentialsProvider(awsCredentialProvider);
    //        return new KinesisProducer(kinesisProducerConfig);
    //    }
}

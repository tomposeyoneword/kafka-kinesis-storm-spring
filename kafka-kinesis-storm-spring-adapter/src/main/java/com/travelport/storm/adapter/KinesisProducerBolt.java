package com.travelport.storm.adapter;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.travelport.datahub.lib.encryption.Encryption;

// If we want to guarantee message processing,need to use BaseBasicBolt.
// Storm has 3 types of msg processing: at least once, exactly once & at-most-once.
// Useful link:https://bryantsai.com/fault-tolerant-message-processing-in-storm/

@Component
public class KinesisProducerBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -4342594755545333547L;
    private static final Logger LOG = LoggerFactory.getLogger(KinesisProducerBolt.class);

    private static final String VALUE = "value";
    private static final Fields FIELDS = new Fields("message");

    @Value("${encryption.key}")
    private String encryptionKey;

    @Value("${aws.access.key}")
    private String awsAccessKey;

    //    @Value("${aws.secret.key}")
    //    private String awsSecretKey;

    @Value("${aws.secret.key.encrypted}")
    private String awsSecretKeyEncrypted;

    @Value("${aws.producer.config.file}")
    private String kinesisProducerConfigFile;

    @Value("${aws.kinesis.stream.name}")
    private String kinesisStreamName;

    // I assume we should use a static value
    @Value("${aws.kinesis.partition}")
    private String kinesisPartition;

    // If we instantiate objects in the constructor (or by @Autowire), Storm will try to serialize them when deploying the topology.
    // KinesisProducer is an AWS Library class and we have no control over its serializability. Instantiate this in the prepare() call.
    //
    // Kinesis Producer:
    // https://docs.aws.amazon.com/streams/latest/dev/kinesis-producer-adv-retries-rate-limiting.html
    // Records that fail are automatically added back to the KPL buffer.  This strategy allows retried 
    // KPL user records to be included in subsequent Kinesis Data Streams API calls to improve throughput 
    // and reduce complexity while enforcing the Kinesis Data Streams record’s time-to-live value. 
    // There is no backoff algorithm. Spamming due to excessive retries is prevented by rate limiting.
    // The KinesisProducer instance is thread safe, and should be sharable.
    private transient KinesisProducer producer;
    private transient ExecutorService executor;

    //private AtomicLong records = new AtomicLong(0);

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        final String message = input.getStringByField(VALUE);

        while (producer.getOutstandingRecordsCount() > 1000) {
            try {
                LOG.warn("Outstanding Record Count: " + producer.getOutstandingRecordsCount());
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // Do nothing
            }
        }

        //records.getAndIncrement();
        //Runnable publishToKinesis = () -> publish(message);
        //executor.execute(publishToKinesis);

        publish(message);
    }

    public void publish(final String message) {
        final ByteBuffer kinesisMessage = StandardCharsets.UTF_8.encode(message);
        ListenableFuture<UserRecordResult> future = producer.addUserRecord(kinesisStreamName, kinesisPartition, kinesisMessage);
        Futures.addCallback(future, new FutureCallback<UserRecordResult>() {

            @Override
            public void onSuccess(UserRecordResult result) {

                //                if (records.get() % 100 == 0) {
                //                    System.out.println("Success: " + records.get());
                //                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Success=" + message);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof UserRecordFailedException) {
                    UserRecordFailedException failedException = (UserRecordFailedException) t;
                    Attempt last = Iterables.getLast(failedException.getResult().getAttempts());
                    String error = String.format("%s | %s", last.getErrorCode(), last.getErrorMessage());
                    LOG.error("Message failed=" + error + "\nMessage=" + message, t);
                    return;
                }
                LOG.error("Message failed", t);
            }
        });
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(FIELDS);
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {

        // prepare() will only be called once, after the bolt is deployed
        // so we shouldn't have Serializable problems with KinesisProducer

        try {
            String kinesisProducerConfigFilePath = ResourceUtils.getFile(kinesisProducerConfigFile).getPath();
            Encryption encryption = new Encryption(encryptionKey);
            String awsSecretKey = encryption.decrypt(awsSecretKeyEncrypted);
            BasicAWSCredentials awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            AWSStaticCredentialsProvider awsCredentialProvider = new AWSStaticCredentialsProvider(awsCredentials);
            KinesisProducerConfiguration kinesisProducerConfig = KinesisProducerConfiguration
                    .fromPropertiesFile(kinesisProducerConfigFilePath)
                    .setCredentialsProvider(awsCredentialProvider);
            producer = new KinesisProducer(kinesisProducerConfig);

            executor = Executors.newFixedThreadPool(2);

        } catch (FileNotFoundException e) {
            LOG.error("Unable to locate the Kinesis Producer properties file", e);
        }
    }
}

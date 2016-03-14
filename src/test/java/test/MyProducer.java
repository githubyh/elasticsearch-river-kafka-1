package test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
 
public class MyProducer {
    private static final String TOPIC = "rivertopic";
    private static final String CONTENT = "This is a single message";
    private static final String BROKER_LIST = "appserver:9092";
    private static final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder";

    protected static final Log logger = LogFactory.getLog(MyProducer.class); 
    
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("serializer.class", SERIALIZER_CLASS);
        props.put("metadata.broker.list", BROKER_LIST);
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
 
        //Send one message.
//        KeyedMessage<String, String> message = 
//            new KeyedMessage<String, String>(TOPIC, CONTENT);
//        producer.send(message);
        
        //Send multiple messages.
        List<KeyedMessage<String,String>> messages = 
            new ArrayList<KeyedMessage<String, String>>();
        for (int i = 0; i < 1000; i++) {
        	LogBean log = new LogBean();
        	log.setBrowser("ff");
        	log.setEventType("商品新增");
        	log.setLogType("1");
        	log.setRequestUri("product/add.do");
        	log.setUserId("2656");
        	log.setLogBody("We are Chinese, Taiwan is China.");
        	FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        	log.setTimestamp(format.format(new Date()));
        	log.setTimeConsuming(String.valueOf(new Random().nextInt(23456)));
//        	producer.send(new KeyedMessage<String, String>
//           (TOPIC, log.toString()));
            messages.add(new KeyedMessage<String, String>
                (TOPIC, log.toString()));
        }
        
        producer.send(messages);
        System.out.println("===========");
    }
}
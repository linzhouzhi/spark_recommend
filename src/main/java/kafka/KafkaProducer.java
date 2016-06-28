package kafka;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by lzz on 6/14/16.
 */
public class KafkaProducer extends Thread {
    private String topic;

    public KafkaProducer(String topic){
        super();
        this.topic = topic;
    }


    @Override
    public void run() {
        Producer producer = createProducer();
        while(true){
            byte[] buffer=new byte[512];
            try {
                //用户输入
                System.in.read(buffer);
                String str=new String(buffer);
                String str_format = str.replaceAll("[\\t\\n\\r]", "");
                producer.send(new KeyedMessage<Integer, String>(topic, str_format ));
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Producer createProducer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "192.168.1.221:2181,192.168.1.222:2181,192.168.1.223:2181");//声明zk
        properties.put("serializer.class", StringEncoder.class.getName());
        properties.put("metadata.broker.list", "192.168.1.223:9093,192.168.1.223:9094");// 声明kafka broker
        return new Producer<Integer, String>(new ProducerConfig(properties));
    }


    public static void main(String[] args) {
        new KafkaProducer("my-replicated-topic").start();// 使用kafka集群中创建好的主题 test

    }
}

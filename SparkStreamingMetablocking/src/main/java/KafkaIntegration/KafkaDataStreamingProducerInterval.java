package KafkaIntegration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import DataStructures.EntityProfile;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

public class KafkaDataStreamingProducerInterval {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
//        int[] timers = {10, 25, 50, 100, 250};
//        int[] timers = {20000};
//        Random random = new Random();
        
        //TOPIC
        final String topicName = "mytopic";
        
        //CHOOSE THE INPUT PATHS
//        String INPUT_PATH1 = "inputs/dataset1_amazon";
//        String INPUT_PATH2 = "inputs/dataset2_gp";
        
        String INPUT_PATH1 = "inputs/dataset1_abt";
        String INPUT_PATH2 = "inputs/dataset2_buy";

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ArrayList<EntityProfile> EntityListSource = null;
        ArrayList<EntityProfile> EntityListTarget = null;
        
		// reading the files
		ObjectInputStream ois1;
		ObjectInputStream ois2;
		try {
			ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH1));
			ois2 = new ObjectInputStream(new FileInputStream(INPUT_PATH2));
			EntityListSource = (ArrayList<EntityProfile>) ois1.readObject();
			EntityListTarget = (ArrayList<EntityProfile>) ois2.readObject();
			ois1.close();
			ois2.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int uniqueId = 0;
		int stop = 200;
		for (int i = 0; i < Math.max(EntityListSource.size(), EntityListTarget.size()); i++) {
			if (i < EntityListSource.size()) {
				EntityProfile entitySource = EntityListSource.get(i);
				entitySource.setSource(true);
				entitySource.setKey(uniqueId);
				ProducerRecord<String, String> record = new ProducerRecord<>(topicName, entitySource.getStandardFormat());
	            producer.send(record);
			}
			
			if (i < EntityListTarget.size()) {
				EntityProfile entityTarget = EntityListTarget.get(i);
				entityTarget.setSource(false);
				entityTarget.setKey(uniqueId);
				ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, entityTarget.getStandardFormat());
	            producer.send(record2);
			}
			
			uniqueId++;
			
			if (uniqueId == stop) {
				stop += 200;
				Thread.sleep(20000);//sleep for 12 sec.
			}
		}
		
        producer.close();
        System.out.println("----------------------------END------------------------------");
    }
}

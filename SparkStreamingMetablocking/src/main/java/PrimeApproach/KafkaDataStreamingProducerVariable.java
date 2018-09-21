package PrimeApproach;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import DataStructures.EntityProfile;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

//localhost:9092 400 inputs/dataset1_abt inputs/dataset2_buy
public class KafkaDataStreamingProducerVariable {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);//localhost:9092    10.171.171.50:8088
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
//        int[] timers = {10, 25, 50, 100, 250};
        //CHOOSE THE INPUT PATHS
        String INPUT_PATH1 = args[1];
        boolean IS_SOURCE = Boolean.parseBoolean(args[2]);
        int window = Integer.parseInt(args[3]);
        int type = Integer.parseInt(args[4]);
        int size = Integer.parseInt(args[5]);
        int[] timerSet = new int[size];
        for (int i = 0; i < size; i++) {
        	timerSet[i] = Integer.parseInt(args[i+6]);
		}
        
        Random random = new Random();
        
        //TOPIC
        final String topicName = "mytopic";
        
        
//        String INPUT_PATH2 = args[3];
        
//        String INPUT_PATH1 = "inputs/dataset1_imdb";
//        String INPUT_PATH2 = "inputs/dataset2_dbpedia";
        
//        String INPUT_PATH1 = "inputs/dataset1_dblp2";
//        String INPUT_PATH2 = "inputs/dataset2_scholar";
        
//        String INPUT_PATH1 = "inputs/dataset1_dblp";
//        String INPUT_PATH2 = "inputs/dataset2_acm";
        
//        String INPUT_PATH1 = "inputs/dataset1_amazon";
//        String INPUT_PATH2 = "inputs/dataset2_gp";
        
//        String INPUT_PATH1 = "inputs/dataset1_abt";
//        String INPUT_PATH2 = "inputs/dataset2_buy";

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ArrayList<EntityProfile> EntityListSource = null;
//        ArrayList<EntityProfile> EntityListTarget = null;
        
		// reading the files
		ObjectInputStream ois1;
		ObjectInputStream ois2;
		try {
			ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH1));
//			ois2 = new ObjectInputStream(new FileInputStream(INPUT_PATH2));
			EntityListSource = (ArrayList<EntityProfile>) ois1.readObject();
//			EntityListTarget = (ArrayList<EntityProfile>) ois2.readObject();
			ois1.close();
//			ois2.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int uniqueId = 0;
//		for (int i = 0; i < Math.max(EntityListSource.size(), EntityListTarget.size()); i++) {
		int index = 0;
		double sum = 0;
		for (int i = 0; i < EntityListSource.size(); i++) {
			if (i < EntityListSource.size()) {
				EntityProfile entitySource = EntityListSource.get(i);
				entitySource.setSource(IS_SOURCE);
				entitySource.setKey(uniqueId);
				ProducerRecord<String, String> record;
				if (type == 1) {
					record = new ProducerRecord<>(topicName, entitySource.getStandardFormat());
				} else {
					record = new ProducerRecord<>(topicName, entitySource.getStandardFormat2());
				}
				
	            producer.send(record);
			}
			
//			if (i < EntityListTarget.size()) {
//				EntityProfile entityTarget = EntityListTarget.get(i);
//				entityTarget.setSource(false);
//				entityTarget.setKey(uniqueId);
//				ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, entityTarget.getStandardFormat());
//	            producer.send(record2);
//			}
			
			
			if (index < size && uniqueId >= (sum + timerSet[index])) {
				sum += timerSet[index];
				System.out.println("bacth" + index + ": " + uniqueId);
				index++;
				Thread.sleep(window * 1000);
			}
			
			uniqueId++;
            
		}
		
        producer.close();
        System.out.println("----------------------------END------------------------------");
    }
}
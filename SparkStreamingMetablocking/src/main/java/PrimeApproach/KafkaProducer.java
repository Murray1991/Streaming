package PrimeApproach;

import DataStructures.EntityProfile;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Properties;

/**
 @author Leonardo Gazzarri

 Produces batches of DatasetSize1/args[4]+DatasetSize2/args[4]
 Second argument args[1] is ignored
 **/

//localhost:9092 400 inputs/dataset1_abt inputs/dataset2_buy
public class KafkaProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);//localhost:9092    10.171.171.50:8088
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //TOPIC
        final String topicName = "mytopic";
        
        //CHOOSE THE INPUT PATHS
        String INPUT_PATH1 = args[2];
        String INPUT_PATH2 = args[3];
        
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

        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
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
		int batchSize = Integer.parseInt(args[4]);
		int currentSize = batchSize;
		for (int i = 0; i < Math.max(EntityListSource.size(), EntityListTarget.size()); i++) {
			if (i < EntityListSource.size()) {
				EntityProfile entitySource = EntityListSource.get(i);
				entitySource.setSource(true);
				entitySource.setKey(uniqueId);
				ProducerRecord<String, String> record = new ProducerRecord<>(topicName, entitySource.getStandardFormat());
				System.out.println("Send 1 - "+uniqueId);
				producer.send(record);
			}

			if (i < EntityListTarget.size()) {
				EntityProfile entityTarget = EntityListTarget.get(i);
				entityTarget.setSource(false);
				entityTarget.setKey(uniqueId);
				ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, entityTarget.getStandardFormat());
				System.out.println("Send 2 - "+uniqueId);
				producer.send(record2);
			}

			uniqueId++;
		}

        producer.close();
        System.out.println("----------------------------END------------------------------");
    }
}

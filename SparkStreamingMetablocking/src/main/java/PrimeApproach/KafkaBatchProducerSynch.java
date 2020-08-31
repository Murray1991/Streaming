package PrimeApproach;

import DataStructures.EntityProfile;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

/**
 @author Leonardo Gazzarri

 Produces batches of DatasetSize1/args[4]+DatasetSize2/args[4]
 Second argument args[1] is ignored
 **/

//localhost:9092 400 inputs/dataset1_abt inputs/dataset2_buy
public class KafkaBatchProducerSynch {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);//localhost:9092    10.171.171.50:8088
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		int time = Integer.parseInt(args[1]) * 1000;

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
		System.out.println("D1 size:"+EntityListSource.size());
		System.out.println("D2 size:"+EntityListTarget.size());
		double size1 = EntityListSource.size();
		double size2 = EntityListTarget.size();
		int nBatches = Integer.parseInt(args[4]);

//		int batchSize1 = (int) Math.ceil(size1/(double)nBatches) +1;
//		int batchSize2 = (int) Math.ceil(size2/(double)nBatches) +1;
//
//		int batches = Integer.parseInt(args[4]);
//		for (int i = 0; i < nBatches; i++) {
//			System.out.println("Send batch #"+i+" of size "+batchSize1);
//			for (int j = 0; j < batchSize1; j++) {
//				int idx = i * batchSize1 + j;
//				if (idx < EntityListSource.size()) {
//					EntityProfile entitySource = EntityListSource.get(idx);
//					entitySource.setSource(true);
//					entitySource.setKey(idx);
//					ProducerRecord<String, String> record = new ProducerRecord<>(topicName, entitySource.getStandardFormat());
//					producer.send(record);
//				}
//			}
//
//			System.out.println("Send batch #"+i+" of size "+batchSize2);
//
//			for (int j = 0; j < batchSize2; j++) {
//				int idx = i * batchSize2 + j;
//				if (idx < EntityListTarget.size()) {
//					EntityProfile entityTarget = EntityListTarget.get(idx);
//					entityTarget.setSource(false);
//					entityTarget.setKey(idx);
//					ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, entityTarget.getStandardFormat());
//					producer.send(record2);
//				}
//			}
//
//			Thread.sleep(time);
//		}

		int batchSize1 = (int) Math.ceil(size1/(double)nBatches) +1;
		int batchSize2 = (int) Math.ceil(size2/(double)nBatches) +1;
		int currentSize1 = batchSize1;
		int currentSize2 = batchSize2;

		for (int i = 0; i < nBatches; i++) {
			ArrayList<String> batch = new ArrayList<>();

			for (int j = 0; j < batchSize1; j++) {
				int idx = i * batchSize1 + j;
				if (idx < EntityListSource.size()) {
					EntityProfile entitySource = EntityListSource.get(idx);
					entitySource.setSource(true);
					entitySource.setKey(idx);
					batch.add(entitySource.getStandardFormat());
				}
			}

			for (int j = 0; j < batchSize2; j++) {
				int idx = i * batchSize2 + j;
				if (idx < EntityListTarget.size()) {
					EntityProfile entityTarget = EntityListTarget.get(idx);
					entityTarget.setSource(false);
					entityTarget.setKey(idx);
					batch.add(entityTarget.getStandardFormat());
				}
			}

			System.out.println("Send batch #"+i+" of size "+batch.size());
			for (String s : batch) {
				ProducerRecord<String, String> record = new ProducerRecord<>(topicName, s);
				producer.send(record);
			}

			Thread.sleep(time);
		}


        producer.close();
        System.out.println("----------------------------END------------------------------");
    }
}

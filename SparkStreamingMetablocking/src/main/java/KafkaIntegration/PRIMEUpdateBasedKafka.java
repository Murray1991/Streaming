package KafkaIntegration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka.KafkaUtils;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import kafka.serializer.StringDecoder;
import scala.Tuple2;
import streaming.util.CSVFileStreamGeneratorER;
import streaming.util.CSVFileStreamGeneratorPMSD;
import streaming.util.JavaDroppedWordsCounter;
import streaming.util.JavaWordBlacklist;


//Parallel-based Metablockig for Streaming Data
public class PRIMEUpdateBasedKafka {
  public static void main(String[] args) throws InterruptedException {
    //
    // The "modern" way to initialize Spark is to create a SparkSession
    // although they really come from the world of Spark SQL, and Dataset
    // and DataFrame.
    //
    SparkSession spark = SparkSession
        .builder()
        .appName("streaming-Filtering")
        .master("local[4]")
        .getOrCreate();
    
    //
    // Operating on a raw RDD actually requires access to the more low
    // level SparkContext -- get the special Java version for convenience
    //
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    

    // streams will produce data every second (note: it would be nice if this was Java 8's Duration class,
    // but it isn't -- it comes from org.apache.spark.streaming)
    JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
    //checkpointing is necessary since states are used
    ssc.checkpoint("checkpoints/");
    

    //kafka pool to receive streaming data 
    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", "localhost:9092");
    Set<String> topics = Collections.singleton("PRIMEtopic");

    JavaPairInputDStream<String, String> streamOfRecords = KafkaUtils.createDirectStream(ssc,
            String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

    // use a simple transformation to create a derived stream -- the original stream of Records is parsed
    // to produce a stream of KeyAndValue objects
    JavaDStream<EntityProfile> streamOfItems = streamOfRecords.map(s -> new EntityProfile(s._2()));
    

    JavaPairDStream<String, EntityProfile> streamOfPairs =
        streamOfItems.flatMapToPair(new PairFlatMapFunction<EntityProfile, String, EntityProfile>() {
			@Override
			public Iterator<Tuple2<String, EntityProfile>> call(EntityProfile se) throws Exception {
				Set<Tuple2<String, EntityProfile>> output = new HashSet<Tuple2<String, EntityProfile>>();
				
				Set<String> cleanTokens = new HashSet<String>();
				
				for (Attribute att : se.getAttributes()) {
					String[] tokens = att.getValue().split(" ");
					Collections.addAll(cleanTokens, tokens);
				}
				
				for (String tk : cleanTokens) {
					output.add(new Tuple2<>(tk, se));
				}
				
				return output.iterator();
			}
		});
    
    JavaPairDStream<String, Iterable<EntityProfile>> streamGrouped = streamOfPairs.groupByKey();

    
    //START THE METABLOCKING
    //coloca as tuplas no formato <e1, b1>
    JavaPairDStream<String, String> pairEntityBlock = streamGrouped.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<EntityProfile>>, String, String>() {

		@Override
		public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<EntityProfile>> input) throws Exception {
			Set<Tuple2<String, String>> output = new HashSet<Tuple2<String, String>>();
			
			for (EntityProfile streamingEntity : input._2()) {
				String[] urlSplit = streamingEntity.getEntityUrl().split("/");
				Tuple2<String, String> pair = new Tuple2<String, String>(streamingEntity.hashCode() + "/" + urlSplit[urlSplit.length-1], input._1());
				output.add(pair);
			}
			
			return output.iterator();
		}
	});
    
    
    //coloca as tuplas no formato <e1, [b1,b2]>
    JavaPairDStream<String, Iterable<String>> entitySetBlocks = pairEntityBlock.groupByKey();
    
    
    //coloca as tuplas no formato <b1, (e1, b1, b2)>
    JavaPairDStream<String, List<String>> blockEntityAndAllBlocks = entitySetBlocks.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, List<String>>() {
		
    	@Override
		public Iterator<Tuple2<String, List<String>>> call(Tuple2<String, Iterable<String>> input) throws Exception {
			List<Tuple2<String, List<String>>> output = new ArrayList<Tuple2<String, List<String>>>();
			
			List<String> listOfBlocks = StreamSupport.stream(input._2().spliterator(), false).collect(Collectors.toList());
			listOfBlocks.add(0, input._1());
			
			for (String block : input._2()) {
				Tuple2<String, List<String>> pair = new Tuple2<String, List<String>>(block, listOfBlocks);
				output.add(pair);
			}
			return output.iterator();
		}
	});
    
    
    //coloca as tuplas no formato <b1, [(e1, b1, b2), (e2, b1), (e3, b1, b2)]>
    JavaPairDStream<String, Iterable<List<String>>> blockPreprocessed = blockEntityAndAllBlocks.groupByKey();
    
    
    //print the entities in each stream
//    blockPreprocessed.foreachRDD(new VoidFunction<JavaPairRDD<String,Iterable<List<String>>>>() {
//		
//		@Override
//		public void call(JavaPairRDD<String, Iterable<List<String>>> t) throws Exception {
//			System.out.println("--------------New blocks---------------");
//			t.foreach(new VoidFunction<Tuple2<String,Iterable<List<String>>>>() {
//				
//				@Override
//				public void call(Tuple2<String, Iterable<List<String>>> t) throws Exception {
//					System.out.println(t);
//					
//				}
//			});
//			
//		}
//	});
    
    
    
    Function3<String, Optional<Iterable<List<String>>>, State<List<List<String>>>, Tuple2<String, List<List<String>>>> 
    			mappingFunctionBlockPreprocessed = (key, listBlocks, state) -> {
    	List<List<String>> count = (state.exists() ? state.get() : new ArrayList<List<String>>());
    	List<List<String>> listOfBlocks = StreamSupport.stream(listBlocks.get().spliterator(), false).collect(Collectors.toList());
    	count.addAll(listOfBlocks);
        Tuple2<String, List<List<String>>> thisOne = new Tuple2<>(key, count);
        state.update(count);
        return thisOne;
    };
    
    //save in state.
    //Using mapWithState, we just manipulate the update entities/blocks. It's a property provided by mapWithState. UpdateState manipulates with all data (force brute).
    JavaMapWithStateDStream<String, Iterable<List<String>>, List<List<String>>, Tuple2<String, List<List<String>>>> finalOutputProcessed =
    		blockPreprocessed.mapWithState(StateSpec.function(mappingFunctionBlockPreprocessed));
    


//    //print the entities stored in state part1
//    DStream<Tuple2<String, List<List<String>>>> fin_Counts = finalOutputProcessed.dstream();
//    JavaDStream<Tuple2<String, List<List<String>>>> javaDStream = 
//    		   JavaDStream.fromDStream(fin_Counts,
//    		                                    scala.reflect.ClassTag$.MODULE$.apply(String.class));
//    //print the entities stored in state part2
//    javaDStream.foreachRDD(new VoidFunction<JavaRDD<Tuple2<String,List<List<String>>>>>() {
//		
//		@Override
//		public void call(JavaRDD<Tuple2<String, List<List<String>>>> t) throws Exception {
//			System.out.println("--------------Accumulado Completo---------------");
//			t.foreach(new VoidFunction<Tuple2<String,List<List<String>>>>() {
//				
//				@Override
//				public void call(Tuple2<String, List<List<String>>> t) throws Exception {
//					System.out.println(t);
//					
//				}
//			});
//			
//		}
//	});
    
    
    
    
    
    
    //convert to JavaPairDStream
    JavaDStream<Tuple2<String, List<List<String>>>> onlyUpdatedEntityBlocksToCompare = finalOutputProcessed.filter(new Function<Tuple2<String,List<List<String>>>, Boolean>() {
		@Override
		public Boolean call(Tuple2<String, List<List<String>>> v1) throws Exception {
			return true;
		}
	});
    		
    Accumulator<Integer> numberOfComparisons = sc.accumulator(0);
    
	//coloca as tuplas no formato <e1, e2 = 0.65> (calcula similaridade)
    JavaPairDStream<String, String> similarities = onlyUpdatedEntityBlocksToCompare.flatMapToPair(new PairFlatMapFunction<Tuple2<String,List<List<String>>>, String, String>() {
    	
		@Override
		public Iterator<Tuple2<String, String>> call(Tuple2<String, List<List<String>>> input) throws Exception {
			
			List<Tuple2<String, String>> output = new ArrayList<Tuple2<String, String>>();
			
			List<List<String>> listOfEntitiesToCompare = StreamSupport.stream(input._2().spliterator(), false).collect(Collectors.toList());
			
			for (int i = 0; i < listOfEntitiesToCompare.size(); i++) {
				List<String> ent1 = listOfEntitiesToCompare.get(i);
				for (int j = i+1; j < listOfEntitiesToCompare.size(); j++) {
					List<String> ent2 = listOfEntitiesToCompare.get(j);
					if (ent1.size() >= 2 && ent2.size() >= 2) {
						String idEnt1 = ent1.get(0);
						String idEnt2 = ent2.get(0);
						double similarity = calculateSimilarity(ent1, ent2);
						numberOfComparisons.add(1);
						
						
						Tuple2<String, String> pair1 = new Tuple2<String, String>(idEnt1, idEnt2 + "=" + similarity);
						Tuple2<String, String> pair2 = new Tuple2<String, String>(idEnt2, idEnt1 + "=" + similarity);
						output.add(pair1);
						output.add(pair2);
					}
					
				}
			}
			
			return output.iterator();
		}

		private double calculateSimilarity(List<String> ent1, List<String> ent2) {
//    			ent1.remove(0);
//    			ent2.remove(0);
			
			int maxSize = Math.max(ent1.size()-1, ent2.size()-1);
			List<String> intersect = new ArrayList<String>(ent1);
			intersect.retainAll(ent2);
			
			
			if (maxSize > 0) {
				double x = (double)intersect.size()/maxSize;
//				if (x>1) {
//					System.out.println();
//				}
				return x;
			} else {
				return 0;
			}
			
		}
	});
    
    //coloca as tuplas no formato <e1, [(e2 = 0.65), (e3 = 0.8)]>
    JavaPairDStream<String, Iterable<String>> similaritiesGrouped = similarities.groupByKey();
    
    
    
    
    //pruning phase
    JavaPairDStream<String, String> prunedOutput = similaritiesGrouped.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {

		@Override
		public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
			Set<Tuple2<String, String>> output = new HashSet<Tuple2<String, String>>();
			
			double totalWeight = 0;
			double size = 0;
			
			for (String value : tuple._2()) {
				String[] entityWeight = value.split("\\=");
				totalWeight += Double.parseDouble(entityWeight[1]);
				size++;
			}
			
			double pruningWeight = totalWeight/size;
			
			for (String value : tuple._2()) {
				double weight = Double.parseDouble(value.split("\\=")[1]);
				if (weight >= pruningWeight) {
//					if (weight == 0) {
//						System.out.println();
//					}
					output.add(new Tuple2<String, String>(tuple._1(), value));
				}
			}
			
			return output.iterator();
		}
	});
    
    JavaPairDStream<String, Iterable<String>> groupedPruned = prunedOutput.groupByKey();
    		
    groupedPruned.foreachRDD(rdd -> {
    	System.out.println("Number of Comparisons: " + numberOfComparisons.value());
        System.out.println("Batch size: " + rdd.count());
//        rdd.foreach(e -> System.out.println(e));
      });
    
    
    
    // start streaming
    System.out.println("*** about to start streaming");
    ssc.start();
    ssc.awaitTermination();
    System.out.println("*** Streaming terminated");
  }

}
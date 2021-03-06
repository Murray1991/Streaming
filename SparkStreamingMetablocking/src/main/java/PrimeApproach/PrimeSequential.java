package PrimeApproach;

import DataStructures.*;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.util.hash.Hash;
import scala.Tuple2;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


//Parallel-based Metablockig for Streaming Data
//20 localhost:9092 60
public class PrimeSequential {

	final public static HashMap<Integer, TextModel> index0 = new HashMap<>();
	final public static HashMap<Integer, TextModel> index1 = new HashMap<>();

	final public static BlockCollection state = new BlockCollection();
	public static int numberOfComparisons = 0;

	public static TextModel getEntity(int dIdx, int idx) {
		if (dIdx == 0) {
			return index0.get(idx);
		} else return index1.get(idx);
	}

	public static void putEntity(int dIdx, int idx, EntityProfile p) {
		if (dIdx == 0) {
			if (!index0.containsKey(idx))
				index0.put(idx, new TextModel(p));
		} else if (!index1.containsKey(idx))
			index1.put(idx, new TextModel(p));
	}

	public static ArrayList<EntityProfile> readDataset(String INPUT_PATH) throws InterruptedException {
		ArrayList<EntityProfile> EntityList = null;

		// reading the files
		ObjectInputStream ois1;
		ObjectInputStream ois2;
		try {
			ois1 = new ObjectInputStream(new FileInputStream(INPUT_PATH));
			EntityList = (ArrayList<EntityProfile>) ois1.readObject();
			ois1.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return EntityList;
	}

	public static void jaccardSimilarity(Node n) {
		int dIdx = n.isSource() ? 0 : 1;
		TextModel e1 = getEntity(dIdx, n.getId());
		final HashMap<String, Integer> itemsFrequency = e1.getItemsFrequency();

		Set<Tuple2<Integer, Double>> updatedNeighbors = new HashSet<>();
		for (Tuple2<Integer, Double> neighbor : n.getNeighbors()) {
			int dIdx2 = n.isSource() ? 1 : 0;
			TextModel e2 = getEntity(dIdx2, neighbor._1());

			final Set<String> commonKeys = new HashSet<>(itemsFrequency.keySet());
			commonKeys.retainAll(e2.getItemsFrequency().keySet());

			double numerator = commonKeys.size();
			double denominator = itemsFrequency.size() + e2.getItemsFrequency().size() - numerator;
			numberOfComparisons++;
			updatedNeighbors.add(new Tuple2<>(neighbor._1(), numerator/denominator));
		}

		n.setNeighbors(updatedNeighbors);
	}

	private static double calculateSimilarity(Integer blockKey, Set<Integer> ent1, Set<Integer> ent2) {
		int maxSize = Math.max(ent1.size() - 1, ent2.size() - 1);
		Set<Integer> intersect = new HashSet<Integer>(ent1);
		intersect.retainAll(ent2);

		// MACOBI strategy
		if (!Collections.min(intersect).equals(blockKey)) {
			return -1;
		}

		if (maxSize > 0) {
			double x = (double) intersect.size() / maxSize;
			return x;
		} else {
			return 0;
		}
	}

	public static List<Node> process(ArrayList<EntityProfile> entities) {

		//BlockCollection state = new BlockCollection();

		List<Tuple2<Integer, Node>> streamOfPairs = entities.stream().flatMap(se -> {

			List<Tuple2<Integer, Node>> output = new ArrayList<Tuple2<Integer, Node>>();

			Set<Integer> cleanTokens = new HashSet<Integer>();

			for (Attribute att : se.getAttributes()) {
//				String[] tokens = gr.demokritos.iit.jinsect.utils.splitToWords(att.getValue());
				KeywordGenerator kw = new KeywordGeneratorImpl();
				for (String string : kw.generateKeyWords(att.getValue())) {
					cleanTokens.add(string.hashCode());
				}
			}

			Node node = new Node(se.getKey(), cleanTokens, new HashSet<>(), se.isSource());
			int dIdx = node.isSource()? 0: 1;
			putEntity(dIdx, node.getId(), se);

			for (Integer tk : cleanTokens) {
				node.setTokenTemporary(tk);
				output.add(new Tuple2<Integer, Node>(tk, node.clone()));
			}

			return output.stream();
		}).collect(Collectors.toList());


		Map<Integer, List<Tuple2<Integer, Node>>> entityBlocks = streamOfPairs.stream()
				.collect(Collectors.groupingBy(Tuple2::_1));


		List<Tuple2<Integer, NodeCollection>> storedBlocks = entityBlocks.entrySet().stream().map(e -> {
			Integer key = e.getKey();
			List<Tuple2<Integer,Node>> listOfBlocks = e.getValue();

			NodeCollection count = state.exists(key.intValue()) ? state.get(key.intValue()) : new NodeCollection();
			//NodeCollection count = (state.exists() ? state.get() : new NodeCollection());
			//	count.removeOldNodes(Integer.parseInt(args[2]));//time in seconds

			for (Node entBlocks : count.getNodeList()) {
				entBlocks.setMarked(false);
			}

			for (Tuple2<Integer, Node> entBlocks : listOfBlocks) {
				entBlocks._2().setMarked(true);
				count.add(entBlocks._2());
			}

			Tuple2<Integer, NodeCollection> thisOne = new Tuple2<>(key, count);

			state.update(key.intValue(), count.clone());

			// Result = 0
			//for (Node n : thisOne._2().getNodeList())
			//	numberOfComparisons += (n.getNumberOfNeighbors());
				//numberOfComparisons += (n.getNeighbors().size());

			return thisOne;
		}).collect(Collectors.toList());

		// TODO check if it can be done using only current state
		List<Tuple2<Integer, Node>> pairEntityBlock = storedBlocks.stream().flatMap(t -> {
			List<Node> entitiesToCompare = t._2().getNodeList();
			List<Tuple2<Integer, Node>> output = new ArrayList<Tuple2<Integer, Node>>();

			// Result = 4205096
			//for (Node n : entitiesToCompare){
			//	numberOfComparisons += (n.getNeighbors().size());
			//}

			for (int i = 0; i < entitiesToCompare.size(); i++) {
				Node n1 = entitiesToCompare.get(i);
				//if (n1.isSource()) {
				//	if (n1.getNumberOfNeighbors() > 0) {
				//		numberOfComparisons+=n1.getNeighbors().size();
				//	}
				//}
				for (int j = i+1; j < entitiesToCompare.size(); j++) {
					Node n2 = entitiesToCompare.get(j);
					//Only compare nodes from distinct sources and marked as new (avoid recompute comparisons)
					if (n1.isSource() != n2.isSource() && (n1.isMarked() || n2.isMarked())) {
						double similarity = calculateSimilarity(t._1(), n1.getBlocks(), n2.getBlocks());
						if (similarity >= 0) {
							if (n1.isSource()) {
								n1.addNeighbor(new Tuple2<Integer, Double>(n2.getId(), similarity));
							} else {
								n2.addNeighbor(new Tuple2<Integer, Double>(n1.getId(), similarity));
							}
						}
					}
				}
			}

			for (Node node : entitiesToCompare) {
				if (node.isSource()) {
					//numberOfComparisons+=(node.getNeighbors().size());
					output.add(new Tuple2<Integer, Node>(node.getId(), node));
				}
			}
			//System.out.println("Output:" + output.size());
			return output.stream();

		}).collect(Collectors.toList());

		//A beginning block is generated, notice that we have all the Neighbors of each entity.
		Map<Integer, List<Tuple2<Integer,Node>>> graph = pairEntityBlock.stream()
				.collect(Collectors.groupingBy(Tuple2::_1));

		List<Node> prunedGraph = graph.entrySet().stream().map(e -> {

			Integer key = e.getKey();
			List<Tuple2<Integer, Node>> nodes = e.getValue();
//			nodes.sort((Tuple2<Integer,Node> c1, Tuple2<Integer,Node> c2) -> {
//				return Integer.compare(c1._2().getId(), c2._2().getId());
//			});

			Node n1 = nodes.get(0)._2();//get the first node to merge with others.
			for (int j = 1; j < nodes.size(); j++) {
				Node n2 = nodes.get(j)._2();
				n1.addSetNeighbors(n2);
			}

			n1.pruning();

			return n1;
			//return ""+nodes.get(0)._2.getId();
		}).collect(Collectors.toList());

		return prunedGraph;

	}

  public static void main(String[] args) throws InterruptedException {

	  final ArrayList<EntityProfile> EntityListSource = readDataset(args[0]);
	  final ArrayList<EntityProfile> EntityListTarget = readDataset(args[1]);

	  System.out.println("D1 size:"+EntityListSource.size());
	  System.out.println("D2 size:"+EntityListTarget.size());

	  int uniqueId = 0;
	  double size1 = EntityListSource.size();
	  double size2 = EntityListTarget.size();
	  int nBatches = Integer.parseInt(args[2]);

	  int batchSize1 = (int) Math.ceil((double) size1/ (double) nBatches);
	  int batchSize2 = (int) Math.ceil((double) size2/ (double) nBatches);

	  final int chunkSize1 = batchSize1;
	  final int chunkSize2 = batchSize2;
	  final AtomicInteger counter = new AtomicInteger();

	  final Collection<List<EntityProfile>> groupedEntityListSource = EntityListSource.stream()
			  .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / batchSize1))
			  .values();

	  counter.set(0);

	  final Collection<List<EntityProfile>> groupedEntityListTarget = EntityListTarget.stream()
			  .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / batchSize2))
			  .values();

	  ArrayList<String> csvLines = new ArrayList<>();


	  boolean saveFile = Integer.parseInt(args[3]) == 0 ? false : true;
	  System.out.println("Store file? " + saveFile);

	  long total = 0;
	  long startTime = System.currentTimeMillis();
	  final ArrayList<Node> prunedGraph = new ArrayList<>();
	  final StringBuilder sb = new StringBuilder();

	  final Iterator<List<EntityProfile>>  it1 = groupedEntityListSource.iterator();
	  final Iterator<List<EntityProfile>>  it2 = groupedEntityListTarget.iterator();
      for (int i = 0; i < nBatches; i++) {
		long t1 = System.currentTimeMillis();
      	if (it1.hasNext() && it2.hasNext()) {
      		final List<EntityProfile> p1 = it1.next();
      		final List<EntityProfile> p2 = it2.next();
			int nMax = Math.max(p1.size(), p2.size());
			ArrayList<EntityProfile> batch = new ArrayList<>();
			for (int j = 0; j < nMax ; j++) {
				if (p1.size() > j) {
					int idx = i * batchSize1 + j;
					final EntityProfile entitySource = p1.get(j);
					entitySource.setSource(true);
					entitySource.setKey(idx);
					batch.add(entitySource);
				}
				if (p2.size() > j) {
					int idx = i * batchSize2 + j;
					final EntityProfile entityTarget = p2.get(j);
					entityTarget.setSource(false);
					entityTarget.setKey(idx);
					batch.add(entityTarget);
				}
			}

			System.out.println("Send batch #"+i+" of size "+batch.size());
			final List<Node> nodes = process(batch);

			// Execute
			for (Node n: nodes) {
				jaccardSimilarity(n);
				/*if (saveFile) {
					sb.append(n.getId());
					sb.append(",");
				    sb.append(n.toString());
					sb.append("\n");
				}*/
			}

			//if (saveFile)
			//	prunedGraph.addAll(nodes);

			long t2 = System.currentTimeMillis();
			total += (t2-t1);
			csvLines.add("Pi-Block,"+(i+1)+","+(t2-t1)+","+total+","+numberOfComparisons+",0");
			System.out.println("Time #"+i+": "+(t2-t1)+" ms");
		}
	  }
//
//	  boolean saveFile = Integer.parseInt(args[3]) == 0 ? false : true;
//	  System.out.println("Store file? " + saveFile);
//
//	  int batchSize1 = (int) Math.ceil(size1/(double)nBatches) +1;
//	  int batchSize2 = (int) Math.ceil(size2/(double)nBatches) +1;
//	  int currentSize1 = batchSize1;
//	  int currentSize2 = batchSize2;
//
//	  ArrayList<String> csvLines = new ArrayList<>();
//
//	  long total = 0;
//	  long startTime = System.currentTimeMillis();
//	  ArrayList<Node> prunedGraph = new ArrayList<>();
//	  for (int i = 0; i < nBatches; i++) {
//		  long t1 = System.currentTimeMillis();
//		  ArrayList<EntityProfile> batch = new ArrayList<>();
//
//		  for (int j = 0; j < batchSize1; j++) {
//			  int idx = i * batchSize1 + j;
//			  if (idx < EntityListSource.size()) {
//				  EntityProfile entitySource = EntityListSource.get(idx);
//				  entitySource.setSource(true);
//				  entitySource.setKey(idx);
//				  batch.add(new EntityProfile(entitySource.getStandardFormat()));
//			  }
//		  }
//
//		  for (int j = 0; j < batchSize2; j++) {
//			  int idx = i * batchSize2 + j;
//			  if (idx < EntityListTarget.size()) {
//				  EntityProfile entityTarget = EntityListTarget.get(idx);
//				  entityTarget.setSource(false);
//				  entityTarget.setKey(idx);
//				  batch.add(new EntityProfile(entityTarget.getStandardFormat()));
//			  }
//		  }
//
//		  System.out.println("Send batch #"+i+" of size "+batch.size());
//		  List<Node> nodes = process(batch);
//
//		  // Execute
//		  for (Node n: nodes) {
//		  	jaccardSimilarity(n);
//		  }
//
//		  if (saveFile)
//		  	prunedGraph.addAll(nodes);
//
//		  long t2 = System.currentTimeMillis();
//		  total += (t2-t1);
//		  csvLines.add("Pi-Block,"+(i+1)+","+(t2-t1)+","+total+","+numberOfComparisons+",0");
//		  System.out.println("Time #"+i+": "+(t2-t1)+" ms");
//		  //System.out.println("No of comparisons: "+ numberOfComparisons);
//	  }
	  long endTime = System.currentTimeMillis();
	  System.out.println("End-to-end time: "+(endTime-startTime)+" ms");

	  if (saveFile) {
		  try {
		  	  String file = "./outputs/sequential/b"+nBatches+"/file.txt";
		  	  System.out.println("Store block graph in "+file);
			  PrintWriter pr = new PrintWriter(file);
			  pr.print(sb.toString());
			  //for (Node node : prunedGraph) {
			  //	  String line = node.getId() + "," + node.toString();
			   //	  pr.println(line);
			  //}
			  pr.close();
		  } catch (Exception e) {
			  e.printStackTrace();
			  System.out.println("No such file exists.");
		  }

		  try {
			  String file = "./outputs/b"+nBatches+"/res.txt";
			  PrintWriter pr = new PrintWriter(file);
			  for (String line : csvLines) {
				  pr.println(line);
			  }
			  pr.close();
		  } catch (Exception e) {
			  e.printStackTrace();
			  System.out.println("No such file exists.");
		  }
	  }

  }
}

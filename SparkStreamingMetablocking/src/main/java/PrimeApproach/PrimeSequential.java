package PrimeApproach;

import DataStructures.Attribute;
import DataStructures.EntityProfile;
import DataStructures.Node;
import DataStructures.NodeCollection;
import DataStructures.BlockCollection;
import org.apache.commons.collections.IteratorUtils;
import scala.Tuple2;
import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;


//Parallel-based Metablockig for Streaming Data
//20 localhost:9092 60
public class PrimeSequential {

	public static BlockCollection state = new BlockCollection();
	public static int numberOfComparisons = 0;

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

	public static List<String> process(ArrayList<EntityProfile> entities) {

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
					numberOfComparisons+=(node.getNeighbors().size());
					output.add(new Tuple2<Integer, Node>(node.getId(), node));
				}
			}
			//System.out.println("Output:" + output.size());
			return output.stream();

		}).collect(Collectors.toList());

		//A beginning block is generated, notice that we have all the Neighbors of each entity.
		Map<Integer, List<Tuple2<Integer,Node>>> graph = pairEntityBlock.stream()
				.collect(Collectors.groupingBy(Tuple2::_1));

		List<String> prunedGraph = graph.entrySet().stream().map(e -> {

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

			return n1.getId() + "," + n1.toString();
			//return ""+nodes.get(0)._2.getId();
		}).collect(Collectors.toList());

		return prunedGraph;

	}

  public static void main(String[] args) throws InterruptedException {

	  ArrayList<EntityProfile> EntityListSource = readDataset(args[0]);
	  ArrayList<EntityProfile> EntityListTarget = readDataset(args[1]);

	  System.out.println("D1 size:"+EntityListSource.size());
	  System.out.println("D2 size:"+EntityListTarget.size());

	  int uniqueId = 0;
	  double size1 = EntityListSource.size();
	  double size2 = EntityListTarget.size();
	  int nBatches = Integer.parseInt(args[2]);

	  int batchSize1 = (int) Math.ceil(size1/(double)nBatches) +1;
	  int batchSize2 = (int) Math.ceil(size2/(double)nBatches) +1;
	  int currentSize1 = batchSize1;
	  int currentSize2 = batchSize2;

	  ArrayList<String> prunedGraph = new ArrayList<>();
	  for (int i = 0; i < nBatches; i++) {
		  ArrayList<EntityProfile> batch = new ArrayList<>();

		  for (int j = 0; j < batchSize1; j++) {
			  int idx = i * batchSize1 + j;
			  if (idx < EntityListSource.size()) {
				  EntityProfile entitySource = EntityListSource.get(idx);
				  entitySource.setSource(true);
				  entitySource.setKey(idx);
				  batch.add(new EntityProfile(entitySource.getStandardFormat()));
			  }
		  }

		  for (int j = 0; j < batchSize2; j++) {
			  int idx = i * batchSize2 + j;
			  if (idx < EntityListTarget.size()) {
				  EntityProfile entityTarget = EntityListTarget.get(idx);
				  entityTarget.setSource(false);
				  entityTarget.setKey(idx);
				  batch.add(new EntityProfile(entityTarget.getStandardFormat()));
			  }
		  }

		  System.out.println("Send batch #"+i+" of size "+batch.size());
		  prunedGraph.addAll(process(batch));
		  System.out.println("No of comparisons: "+ numberOfComparisons);
	  }

	  try {
		  PrintWriter pr = new PrintWriter("./outputs/sequential/file.txt");
		  for (String line : prunedGraph) {
			  pr.println(line);
		  }
		  pr.close();
	  } catch (Exception e) {
		  e.printStackTrace();
		  System.out.println("No such file exists.");
	  }


  }
}

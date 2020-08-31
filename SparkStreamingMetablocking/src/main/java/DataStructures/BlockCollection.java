package DataStructures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BlockCollection {

    private HashMap<Integer, NodeCollection> state = new HashMap<>();

    public boolean exists(int key) { return state.containsKey(key); }

    public NodeCollection get(int key) { return state.get(key); }

    public void update(int key, NodeCollection value) { state.put(key, value); }

}

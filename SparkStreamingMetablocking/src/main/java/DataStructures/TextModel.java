package DataStructures;

import java.util.HashMap;
import java.util.Set;

public class TextModel {

    private int id;
    private EntityProfile profile;
    private HashMap<String, Integer> itemsFrequency;

    public TextModel(EntityProfile profile) {
        this.profile = profile;
        this.id = this.profile.getKey();

        Set<Attribute> attributes = this.profile.getAttributes();
        itemsFrequency = new HashMap<>();
        int noOfTotalTerms = 0;

        for (Attribute attr: attributes) {
            String text = attr.getValue();
            final String[] tokens = text.toLowerCase().split("[\\W_]");

            int noOfTokens = tokens.length;
            noOfTotalTerms += noOfTokens;
            for (int j = 0; j < noOfTokens; j++) {
                String feature = text.trim();
                if (0 < feature.length()) {
                    if (!itemsFrequency.containsKey(feature)) {
                        itemsFrequency.put(feature, 1);
                    } else {
                        itemsFrequency.put(feature, itemsFrequency.get(feature) + 1);
                    }
                }
            }
        }
    }

    public HashMap<String, Integer> getItemsFrequency() { return itemsFrequency; }

    public int getId() { return id; }
}

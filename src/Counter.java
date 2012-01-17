import cern.colt.list.IntArrayList;
import cern.colt.map.OpenIntIntHashMap;

// This is a simple hash aggregator for counting signatures
public class Counter implements Comparable {

  String docid, SHA1 = null, seq = null;

  int totalCount;

  // THIS IS A CACHE FOR MINHASH SIGNATURES!
  // CLEARLY AN OPTIMIZATION OF LSH AT THE COST OF MEMORY
  // TAKES l*4 ADDITIONAL BYTES PER DOCUMENT
  int[] minhashes = null;

  OpenIntIntHashMap entries;

  public Counter(String docid) {
    this.docid = docid;
    this.entries = new OpenIntIntHashMap();
    this.totalCount = 0;
  }

  public int[] keySet() {
    int[] keys = new int[entries.size()];
    IntArrayList l = entries.keys();
    for (int i = 0; i < entries.size(); i++)
      keys[i] = l.get(i);
    return keys;
  }

  public int size() {
    return entries.size();
  }

  public boolean containsKey(int key) {
    return entries.containsKey(key);
  }

  public int getCount(int key) {
    return entries.get(key);
  }

  public void incrementCount(int key) {
    entries.put(key, getCount(key) + 1);
    totalCount++;
  }

  // Sort by signature length
  public int compareTo(Object o) {
    if (o instanceof Counter) {
      return Double.compare(((Counter) o).totalCount, this.totalCount);
    }
    return 0;
  }

  public String toString() {
    String s = docid + "=[";
    int[] keys = keySet();
    for (int i = 0; i < size(); i++) {
      s += String.valueOf(keys[i]) + ":" + String.valueOf(getCount(keys[i]))
          + (i < size() - 1 ? ", " : "");
    }
    s += "] @ " + String.valueOf(totalCount);
    return s;
  }
}

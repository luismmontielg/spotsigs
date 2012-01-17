import java.util.HashSet;
import java.util.LinkedHashSet;

import cern.colt.list.IntArrayList;
import cern.colt.map.OpenIntObjectHashMap;

public class LSHTable {

  private MinhashTable[] minhashTables;

  private int k, P;

  private int[] a, b;

  private HashSet<Counter> counters;

  private class MinhashTable extends OpenIntObjectHashMap {

    private int idx; // table id

    private int[] indices; // permutation indices

    // Draw k random permutations for each table
    private MinhashTable(int k, int m, int idx) {
      super(1500000);
      this.idx = idx;
      this.indices = new int[k];
      for (int i = 0; i < k; i++)
        indices[i] = (int) Math.floor(Math.random() * m);
    }

    private int[] getSignature(Counter counter) {
      int[] signature = new int[k];
      int q;
      IntArrayList keys = counter.entries.keys();
      for (int i = 0; i < k; i++) {
        signature[i] = Integer.MAX_VALUE;
        for (int j = 0; j < keys.size(); j++) {
          q = ((a[indices[i]] * keys.get(j)) + b[indices[i]]) % P;
          if (q < signature[i])
            signature[i] = q;
        }
      }
      return signature;
    }

    private int hashCode(int[] signature) {
      int h = 0;
      for (int i = 0; i < signature.length && i < k; i++)
        // h += Math.pow(signature[i], i + 1); // bad for long signatures
        h += signature[i]; // approximate! may create additional collisions
      return h;
    }

    private void put(Counter counter) {
      int hashCode = hashCode(getSignature(counter));
      LinkedHashSet<Counter> bucket = null;
      synchronized (this) {
        bucket = (LinkedHashSet<Counter>) super.get(hashCode);
      }
      if (bucket == null) {
        bucket = new LinkedHashSet<Counter>();
        synchronized (this) {
          super.put(hashCode, bucket);
        }
      }
      synchronized (bucket) {
        bucket.add(counter);
      }
      if (counter.minhashes != null)
        counter.minhashes[idx] = hashCode;
    }

    private LinkedHashSet<Counter> getBucket(Counter counter) {
      LinkedHashSet<Counter> bucket = null;
      // Optimization to avoid redundant minhash computations!
      if (counter.minhashes == null) {
        synchronized (this) {
          bucket = (LinkedHashSet<Counter>) super
              .get(hashCode(getSignature(counter)));
        }
      } else {
        synchronized (this) {
          bucket = (LinkedHashSet<Counter>) super.get(counter.minhashes[idx]);
        }
      }
      return bucket;
    }
  }

  public LSHTable(int k, int l, int m, int d) {
    this.k = k; // number of minhashes to be concatenated
    
    // Initialize array of m random linear projections
    this.a = new int[m];
    this.b = new int[m];
    for (int i = 0; i < m; i++) {
      this.a[i] = 1 + (int) Math.floor(Math.random() * (d - 1));
      this.b[i] = (int) Math.floor(Math.random() * d);
    }
    this.P = getPrime(d);
    // System.out.println("PRIME P=" + P);

    // Array of l minhash tables
    this.minhashTables = new MinhashTable[l];
    for (int i = 0; i < l; i++)
      this.minhashTables[i] = new MinhashTable(k, m, i);

    // Optional cache for minhash signatures
    this.counters = new HashSet<Counter>();
  }

  public void deduplicateIndex(Counter counter) {
    SpotSigsIndexer.incrProc();

    // Remove repeated entries from buckets
    HashSet<Counter> union = new HashSet<Counter>();
    LinkedHashSet<Counter> bucket;
    for (int i = 0; i < minhashTables.length; i++)
      if ((bucket = minhashTables[i].getBucket(counter)) != null)
        union.addAll(bucket);

    // Check for near duplicates
    for (Counter counter2 : union) {
      double sim = 1.0;
      if (counter != counter2
          && (counter.SHA1.equals(counter2.SHA1) || (sim = getJaccard(counter,
              counter2, SpotSigs.confidenceThreshold)) >= SpotSigs.confidenceThreshold)) {
        SpotSigs.dupsFound++;
        synchronized (SpotSigs.duplicates) {
          SpotSigs.duplicates.add(counter);
          SpotSigs.duplicates.add(counter2);
        }
        System.out.println(counter.docid + "\t" + counter2.docid + "\t"
            + String.valueOf(sim));
      }
    }
  }

  public boolean contains(Counter counter) {
    return counters.contains(counter);
  }

  public void put(Counter counter) {
    for (int i = 0; i < minhashTables.length; i++)
      minhashTables[i].put(counter);

    // Optionally cache this Counter object (incl. its minhash)
    // synchronized (counters) {
    // counters.add(counter);
    // }
  }

  // Jaccard similarity generalized for multi-sets (weighted dimensions)
  public static double getJaccard(Counter index1, Counter index2,
      double threshold) {
    double min, max, s_min = 0, s_max = 0, bound = 0;
    double upper_max = Math.max(index1.totalCount, index2.totalCount);
    double upper_union = index1.totalCount + index2.totalCount;
    int c1, c2, s_c1 = 0, s_c2 = 0;

    for (int key : index1.keySet()) {
      c1 = index1.getCount(key);
      c2 = index2.getCount(key);
      min = Math.min(c1, c2);
      max = Math.max(c1, c2);
      s_min += min;
      s_max += max;
      s_c1 += c1;
      s_c2 += c2;

      // Early threshold break for pairwise counter comparison
      bound += max - min;
      if ((upper_max - bound) / upper_max < threshold)
        return 0;
      else if (s_min / upper_union >= threshold)
        return 1;
    }

    return s_min
        / (s_max + (index1.totalCount - s_c1) + (index2.totalCount - s_c2));
  }

  private static int getPrime(int n) {
    while (!isPrime(n))
      n++;
    return n;
  }

  private static boolean isPrime(int n) {
    if (n <= 2)
      return n == 2;
    else if (n % 2 == 0)
      return false;
    for (int i = 3, end = (int) Math.sqrt(n); i <= end; i += 2)
      if (n % i == 0)
        return false;
    return true;
  }
}

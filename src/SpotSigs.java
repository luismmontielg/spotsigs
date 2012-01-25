import java.io.BufferedReader;
import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;

import cern.colt.list.IntArrayList;
import cern.colt.map.OpenIntIntHashMap;
import cern.colt.map.OpenIntObjectHashMap;

public class SpotSigs {

  public static int dupsFound = 0, chains, minSpotSigs, range, allspots = 0,
      k = 6, l = 32;

  public static String dupsFile = "spotsigs_duplicates.txt";
  public static String resultsFile = "spotsigs_results.csv";
  public static String caseName = "No_name";
  
  public static double minIdf = 0.2, maxIdf = 0.85;

  public static HashMap<String, Integer> beadPositions;

  public static double dfThreshold, confidenceThreshold;

  public static String delims = " \t\n\r\f.()\",-:;/\\?!@<>$#&%*+|=_`~'{}[]";

  public static Partition[] partitions;

  public static HashSet<Counter> duplicates;
  
  public static HashSet<String> uniques;
  public static HashSet<String> dups;

  public static HashSet<String> stopwords;

  public static OpenIntIntHashMap globalDFs;

  // Consecutive list of words/tokens per doc
  private ArrayList<String> words;

  // The string content of the current doc
  private StringBuffer content;

  protected static class Partition {

    int idx, begin, end, size, maxLength;

    protected OpenIntObjectHashMap unsortedIndex;

    protected OpenIntObjectHashMap invertedIndex;

    public Partition(int idx, int begin, int end) {
      this.idx = idx;
      this.begin = begin;
      this.end = end;
      this.size = 0;
      this.maxLength = 0;
      this.unsortedIndex = new OpenIntObjectHashMap();
      this.invertedIndex = null;
    }
  }

  protected class Callback extends HTMLEditorKit.ParserCallback {

    // Tokenize HTML char data
    public void handleText(char[] data, int pos) {
      StringTokenizer st = new StringTokenizer(new String(data), delims);
      while (st.hasMoreTokens())
        words.add(st.nextToken().toLowerCase());
    }
  }

  public static Partition getPartition(int length) {
    int i = 0;
    while (i < partitions.length && length > partitions[i].end)
      i++;
    if (i == partitions.length)
      return partitions[partitions.length - 1];
    return partitions[i];
  }

  public void createIndexSingleFile(File file, BufferedReader in) {
    int bytes = 0;

    try {

      String line, docid = file.getPath();
      words = new ArrayList<String>();
      content = new StringBuffer();

      while ((line = in.readLine()) != null) {
        bytes += line.length() + 1;
        content.append(line);
        content.append("\n");
      }

      // Use this to remove HTML markup with the default Java HTMLEditorkit
      // (use a better HTML parser if available!)
      // new ParserDelegator().parse(new StringReader(content.toString()),
      // new Callback(), true);

      // Use this to tokenize the entire content (incl. tags)
      StringTokenizer st = new StringTokenizer(content.toString(), delims);
      while (st.hasMoreTokens())
        words.add(st.nextToken().toLowerCase());

      // Extract Spot-Signatures
      tokenize(words, docid);
      SpotSigsIndexer.incrRead();

    } catch (Exception e) {
      e.printStackTrace();
    }
    SpotSigsIndexer.incrBytes(bytes);
  }

  public void createIndexTREC(BufferedReader in) {
    String docid, line = null;
    int bytes = 0;

    try {

      while ((line = in.readLine()) != null) {
        bytes += line.length();

        if (line.startsWith("<DOC>")) {

          boolean inHTML = false;
          words = new ArrayList<String>();
          content = new StringBuffer();
          docid = null;

          while ((line = in.readLine()) != null) {
            bytes += line.length() + 1;
            if (line.startsWith("<DOCNO>")) {
              docid = line.substring(7, line.indexOf("</DOCNO>")).trim()
                  .toUpperCase();
              continue;
            } else if (line.startsWith("</DOC>")) {
              break;
            } else if (line.startsWith("</DOCHDR>")) {
              inHTML = true;
              continue;
            } else if (!inHTML) {
              continue;
            }
            content.append(line);
            content.append("\n");
          }

          // Use this to remove HTML markup with the default Java HTMLEditorkit
          // (use a better HTML parser if available!)
          // new ParserDelegator().parse(new StringReader(content.toString()),
          // new Callback(), true);

          // Use this to tokenize the entire content (incl. tags)
          StringTokenizer st = new StringTokenizer(content.toString(), delims);
          while (st.hasMoreTokens())
            words.add(st.nextToken().toLowerCase());

          // Extract Spot-Signatures
          tokenize(words, docid);

          SpotSigsIndexer.incrRead();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    SpotSigsIndexer.incrBytes(bytes);
  }

  public void createIndexARC(BufferedReader in) {
    String docid, line = null;
    int bytes = 0;

    try {

      // ARC metadata
      while ((line = in.readLine()) != null
          && !line.startsWith("</arcmetadata>"))
        bytes += line.length() + 1;
      line = in.readLine();

      // Iterate over archive file
      while (line != null) {
        bytes += line.length() + 1;

        // New page
        if (line.startsWith("http://")
            && line.substring(line.indexOf(" ") + 1).startsWith("0.0.0.0")) {

          words = new ArrayList<String>();
          content = new StringBuffer();
          docid = line.substring(0, line.indexOf(" "));

          // HTTP metadata
          while ((line = in.readLine()) != null && line.trim().length() > 0)
            bytes += line.length() + 1;

          // Page content
          while ((line = in.readLine()) != null) {
            bytes += line.length() + 1;
            if (line.startsWith("http://")
                && line.substring(line.indexOf(" ") + 1).startsWith("0.0.0.0"))
              break;
            content.append(line);
            content.append("\n");
          }

          // Use this to remove HTML markup with the default Java HTMLEditorkit
          // (use a better HTML parser if available!)
          // new ParserDelegator().parse(new StringReader(content.toString()),
          // new Callback(), true);

          // Use this to tokenize the entire content (incl. tags)
          StringTokenizer st = new StringTokenizer(content.toString(), delims);
          while (st.hasMoreTokens())
            words.add(st.nextToken().toLowerCase());

          // Extract Spot-Signatures
          tokenize(words, docid);
          SpotSigsIndexer.incrRead();
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    SpotSigsIndexer.incrBytes(bytes);
  }

  public void createIndexWB(BufferedReader in) {
    String docid, line = null;
    int bytes = 0;

    try {

      // Iterate over archive file
      line = in.readLine();
      while (line != null) {
        bytes += line.length() + 1;

        // New page
        if (line.startsWith("==P=>>>>=i===<<<<=T===>=A===<=!Junghoo!==>")) {
          line = in.readLine();
          bytes += line.length() + 1;

          words = new ArrayList<String>();
          content = new StringBuffer();
          docid = line.substring(5);

          // WB metadata
          while ((line = in.readLine()) != null && line.trim().length() > 0)
            bytes += line.length() + 1;

          // HTTP metadata
          while ((line = in.readLine()) != null && line.trim().length() > 0)
            bytes += line.length() + 1;

          // Page content
          while ((line = in.readLine()) != null) {
            bytes += line.length() + 1;
            if (line.startsWith("==P=>>>>=i===<<<<=T===>=A===<=!Junghoo!==>"))
              break;
            content.append(line);
            content.append("\n");
          }

          // Use this to remove HTML markup with the default Java HTMLEditorkit
          // (use a better HTML parser if available!)
          // new ParserDelegator().parse(new StringReader(content.toString()),
          // new Callback(), true);

          // Use this to tokenize the entire content (incl. tags) 
          StringTokenizer st = new StringTokenizer(content.toString(), delims);
          while (st.hasMoreTokens())
            words.add(st.nextToken().toLowerCase());

          // Extract Spot-Signatures
          tokenize(words, docid);
          SpotSigsIndexer.incrRead();
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    SpotSigsIndexer.incrBytes(bytes);
  }

  public void tokenize(ArrayList<String> words, String docid) {
    int i, j, k;

    String word, token;
    StringBuffer chain;
    Integer pos;

    // Hash counter for SpotSigs per doc
    Counter counter = new Counter(docid);
    counter.SHA1 = String.valueOf(content.toString().hashCode());
    try {
      counter.SHA1 = SHA1Table.SHA1(content.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }

    for (i = 0; i < words.size() - 1; i++) {
      word = words.get(i);
      if ((pos = beadPositions.get(word)) != null) {
        chain = new StringBuffer();
        k = i + pos;
        for (j = 0; j < chains && k < words.size(); j++) {
          token = words.get(k);
          while (stopwords.contains(token) && k < words.size()) {
            token = words.get(k);
            k++;
          }
          if (!stopwords.contains(token)) {
            chain.append(token);
            chain.append(":");
          }
          k += pos;
        }
        // i += (k - i);
        if (chain.length() > 0) {
          // Use this to create particle-antecedent pairs as features
          // chain.append(word);
          // Use only this to create particle-only features
          counter.incrementCount(SpotSigsIndexer.getKey(chain.toString()));
        }
      }
    }

    // Threshold for min. amount of SpotSigs
    if (counter.totalCount < minSpotSigs)
      return;

    System.out.println("READ: " + SpotSigsIndexer.read + "\t\t " + docid
        + "\t " + counter.totalCount + " SpotSignatures");

    // Aggregate global DF statistics
    for (int spotSig : counter.keySet()) {
      synchronized (globalDFs) {
        globalDFs.put(spotSig, globalDFs.get(spotSig) + 1);
      }
    }

    // Remember this doc for phase 2
    synchronized (SpotSigsIndexer.counters1) {
      SpotSigsIndexer.counters1.add(counter);
    }
  }

  public void deduplicateIndex(Counter counter) {
    SpotSigsIndexer.incrProc();
    /*
     * synchronized (duplicates) { if (duplicates.contains(counter)) return; }
     */

    Partition partition = getPartition(counter.totalCount);
    Counter[] indexList;
    Counter counter2;
    HashSet<Counter> checked;

    double delta, delta2, sim;
    int i, j, key, iteration, iterations = 1;

    int[] keys = new int[counter.size()];
    int[] dfs = new int[counter.size()];
    IntArrayList keySet = counter.entries.keys();

    // Sort SpotSigs in ascending order of selectivity (DF)
    i = 0;
    for (j = 0; j < counter.size(); j++) {
      key = keySet.get(j);
      synchronized (partition.invertedIndex) {
        indexList = (Counter[]) partition.invertedIndex.get(key);
      }
      keys[i] = key;
      dfs[i] = indexList != null ? indexList.length : Integer.MAX_VALUE;
      i++;
    }
    quicksort(keys, dfs, 0, i - 1);

    // Check for next partition
    if (partition.idx + 1 < partitions.length
        && partition.end - counter.totalCount <= (1 - confidenceThreshold)
            * partition.end)
      iterations = 2;

    // For some borderline docs, (at most) two partitions may come into
    // question!
    for (iteration = 0; iteration < iterations; iteration++) {

      // Do not check the same pair twice!
      checked = new HashSet<Counter>();
      checked.add(counter);

      delta = 0;
      for (i = 0; i < keys.length; i++) {
        if (checked.size() == partition.size)
          break;

        synchronized (partition.invertedIndex) {
          indexList = (Counter[]) partition.invertedIndex.get(keys[i]);
        }
        if (indexList != null) {
          for (j = 0; j < indexList.length; j++) {
            counter2 = indexList[j];

            // Early threshold break for index list traversal
            delta2 = counter.totalCount - counter2.totalCount;
            if (delta2 >= 0
                && delta + delta2 > (1 - confidenceThreshold)
                    * counter.totalCount)
              break;
            else if (counter2 == counter
                || (delta2 < 0 && delta - delta2 > (1 - confidenceThreshold)
                    * counter2.totalCount) || checked.contains(counter2))
              continue;

            // Remember this comparison
            checked.add(counter2);

            // Got a pair of exact or near duplicates!
            sim = 1.0;
            if (counter.SHA1.equals(counter2.SHA1)
                || (sim = getJaccard(keys, counter, counter2,
                    confidenceThreshold)) >= confidenceThreshold) {
              SpotSigs.dupsFound++;
              synchronized (SpotSigs.duplicates) {
                SpotSigs.duplicates.add(counter);
                SpotSigs.duplicates.add(counter2);
              }
              
              synchronized (SpotSigs.uniques) {
            	  if (!SpotSigs.uniques.contains(counter2.docid)) {
            		  SpotSigs.uniques.add(counter.docid);
            		  synchronized (SpotSigs.dups) {
            			  SpotSigs.dups.add(counter2.docid);
            		  }
            	  }
              }
              
              System.out.println(counter.docid + "\t" + counter2.docid + "\t"
                  + String.valueOf(sim));
            }
          }
        }

        // Early threshold break for inverted index traversal
        delta += counter.getCount(keys[i]);
        if (delta > (1 - confidenceThreshold) * partition.maxLength)
          break;
      }

      // Also check this doc against the next partition
      if (iterations == 2 && iteration == 0)
        partition = partitions[partition.idx + 1];
    }
  }

  public double getJaccard(int[] keys, Counter index1, Counter index2,
      double threshold) {
    double min, max, s_min = 0, s_max = 0, bound = 0;
    double upper_max = Math.max(index1.totalCount, index2.totalCount);
    double upper_union = index1.totalCount + index2.totalCount;
    int i, c1, c2, s_c1 = 0, s_c2 = 0;

    for (i = 0; i < keys.length; i++) {
      c1 = index1.getCount(keys[i]);
      c2 = index2.getCount(keys[i]);
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

  private int partition(int[] keys, int[] counts, int low, int high) {
    int i = low - 1;
    int j = high + 1;
    int pivotK = counts[(low + high) / 2];
    while (i < j) {
      i++;
      while (counts[i] < pivotK)
        i++;
      j--;
      while (counts[j] > pivotK)
        j--;
      if (i < j) {
        int tempKey = keys[i];
        keys[i] = keys[j];
        keys[j] = tempKey;
        int tempCount = counts[i];
        counts[i] = counts[j];
        counts[j] = tempCount;
      }
    }
    return j;
  }

  protected synchronized void quicksort(int[] keys, int[] counts, int low,
      int high) {
    if (low >= high)
      return;
    int p = partition(keys, counts, low, high);
    quicksort(keys, counts, low, p);
    quicksort(keys, counts, p + 1, high);
  }
}

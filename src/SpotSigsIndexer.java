import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import cern.colt.list.IntArrayList;
import cern.colt.map.OpenIntIntHashMap;
import cern.colt.map.OpenIntObjectHashMap;

public class SpotSigsIndexer {

  public static int threadId = 0, read = 0, proc = 0, base = 0, numFiles = 0,
      maxFiles = Integer.MAX_VALUE;

  public static long bytes = 0;

  public static int bufferedReadSize = 64 * 1024 * 1024;

  public static int maxThreads = 2;

  public static ArrayList<Counter> counters1, counters2;

  public static ArrayList<File> files;

  public static HashMap<String, Integer> keys;

  public static HashMap<Integer, String> invKeys;

  public static HashSet<Integer> distinctSpots;

  public static String MODE = "SPOTSIGS", FORMAT = "TXT",
      STOPWORD_FILE = "stopwords.txt";

  private static LSHTable lsh;

  private static SHA1Table sha1;

  private Iterator<File> fileIterator;

  private Iterator<SpotSigs.Partition> partitions;

  private Iterator<Counter> indexIterator1, indexIterator2;

  private ArrayList<SpotSigsProcessor> threads;

  public SpotSigsIndexer(ArrayList<File> files) {
    this.threads = new ArrayList<SpotSigsProcessor>();
    this.fileIterator = files.iterator();
    counters1 = new ArrayList<Counter>();
    counters2 = new ArrayList<Counter>();
  }

  private static long parseTime = Long.MAX_VALUE, filterTime = Long.MAX_VALUE,
      invTime = Long.MAX_VALUE, dedupTime = Long.MAX_VALUE, finishTime = 0;

  public void work() throws Exception {
    SpotSigsProcessor thread;
    int idx = 0, i = SpotSigs.minSpotSigs, last = i;
    if (MODE.equals("SPOTSIGS")) {
      ArrayList<SpotSigs.Partition> p_temp = new ArrayList<SpotSigs.Partition>();
      for (i = SpotSigs.minSpotSigs; i <= SpotSigs.range; i++) {
        if (i - last > (1 - SpotSigs.confidenceThreshold) * i) {
          SpotSigs.Partition partition = new SpotSigs.Partition(idx, last, i);
          p_temp.add(partition);
          last = i + 1;
          idx++;
        }
      }
      SpotSigs.Partition partition = new SpotSigs.Partition(idx, last,
          Integer.MAX_VALUE);
      p_temp.add(partition);
      SpotSigs.partitions = new SpotSigs.Partition[p_temp.size()];
      p_temp.toArray(SpotSigs.partitions);
    }

    do {
      while (threads.size() < maxThreads) {
        thread = new SpotSigsProcessor(this, getThreadId());
        threads.add(thread);
        thread.start();
        Thread.yield();
      }
      Thread.sleep(1000);
    } while (threads.size() > 0);

    System.out.println(SpotSigsIndexer.base
        + "\t FILES SELECTED FOR DEDUPLICATION.");
    System.out.println(SpotSigs.duplicates.size()
        + "\t DISTINCT NEAR DUPLICATES DETECTED.");
    System.out
        .println(SpotSigs.dupsFound + "\t NEAR DUPLICATE PAIRS DETECTED.");
    System.out.println((filterTime - parseTime) + "\t MS FOR PARSING.");
    System.out.println((dedupTime - filterTime) + "\t MS FOR SORTING/HASHING.");
    System.out.println((finishTime - dedupTime) + "\t MS FOR DEDUPLICATION.");

    System.exit(0);
  }

  public static synchronized int incrRead() {
    return ++read;
  }

  public static synchronized int incrProc() {
    return ++proc;
  }

  public static synchronized long incrBytes(int b) {
    bytes += b;
    return bytes;
  }

  public static synchronized int getThreadId() {
    return threadId++;
  }

  public static synchronized Integer getKey(String s) {
    Integer i = null;
    if ((i = keys.get(s)) == null) {
      i = new Integer(keys.size() + 1);
      keys.put(s, i);
      invKeys.put(i, s);
    }
    return i;
  }

  public static void main(String[] args) throws Exception {
    processConfigFile(args[1]);
    try {
      maxFiles = Integer.parseInt(args[2]);
    } catch (Exception e) {
      maxFiles = Integer.MAX_VALUE;
    }
    try {
      SpotSigs.minIdf = Double.parseDouble(args[3]);
      System.out.println("minIdf=" + SpotSigs.minIdf);
    } catch (Exception e) {
    }
    try {
      SpotSigs.maxIdf = Double.parseDouble(args[4]);
      System.out.println("maxIdf=" + SpotSigs.maxIdf);
    } catch (Exception e) {
    }
    try {
      SpotSigs.confidenceThreshold = Double.parseDouble(args[5]);
      System.out.println("conf=" + SpotSigs.confidenceThreshold);
    } catch (Exception e) {
    }
    try {
      SpotSigs.k = Integer.parseInt(args[6]);
      System.out.println("k=" + SpotSigs.k);
    } catch (Exception e) {
    }
    try {
      SpotSigs.l = Integer.parseInt(args[7]);
      System.out.println("l=" + SpotSigs.l);
    } catch (Exception e) {
    }
    keys = new HashMap<String, Integer>(1000000);
    invKeys = new HashMap<Integer, String>(1000000);
    files = new ArrayList<File>();
    scanDirs(new File(args[0]));
    numFiles = files.size();
    new SpotSigsIndexer(files).work();
  }

  private static void scanDirs(File file) {
    if (files.size() >= maxFiles)
      return;
    if (file.isFile()) {
      files.add(file);
      // System.out.println(file);
    } else if (file.isDirectory()) {
      File[] list = file.listFiles();
      for (int i = 0; i < list.length; i++)
        scanDirs(list[i]);
    }
  }

  public static void processConfigFile(String configFileName) {
    File configFile = new File(configFileName);
    BufferedReader in = null;
    String line = null;

    try {

      in = new BufferedReader(new FileReader(configFile));
      in.readLine();
      SpotSigs.chains = Integer.parseInt(in.readLine());
      SpotSigs.beadPositions = new HashMap<String, Integer>(100);
      in.readLine();
      while ((line = in.readLine()) != null) {
        if (line.equals("<ANTECEDENTS>"))
          continue;
        if (line.equals("</ANTECEDENTS>"))
          break;
        String[] antecedentInfo = line.split(" ");
        int spotDistance = Integer.parseInt(antecedentInfo[1]);
        SpotSigs.beadPositions
            .put(antecedentInfo[0], new Integer(spotDistance));
      }

      line = in.readLine();
      line = in.readLine();
      SpotSigs.confidenceThreshold = Double.parseDouble(line);

      line = in.readLine();
      line = in.readLine();
      SpotSigs.minSpotSigs = Integer.parseInt(in.readLine());

      line = in.readLine();
      line = in.readLine();
      SpotSigs.range = Integer.parseInt(in.readLine());

      line = in.readLine();
      line = in.readLine();
      SpotSigs.minIdf = Double.parseDouble(in.readLine());

      line = in.readLine();
      line = in.readLine();
      SpotSigs.maxIdf = Double.parseDouble(in.readLine());

      line = in.readLine();
      line = in.readLine();
      SpotSigs.k = Integer.parseInt(in.readLine());

      line = in.readLine();
      line = in.readLine();
      SpotSigs.l = Integer.parseInt(in.readLine());

      line = in.readLine();
      line = in.readLine();
      SpotSigsIndexer.MODE = in.readLine().trim();

      line = in.readLine();
      line = in.readLine();
      SpotSigsIndexer.FORMAT = in.readLine().trim();

      SpotSigs.duplicates = new HashSet<Counter>(1000000);

      SpotSigs.partitions = new SpotSigs.Partition[1];
      SpotSigs.stopwords = new HashSet<String>();
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          SpotSigs.partitions.getClass().getClassLoader().getResource(
              SpotSigsIndexer.STOPWORD_FILE).openStream()));

      String tmp;
      while ((tmp = reader.readLine()) != null) {
        StringTokenizer tokenizer = new StringTokenizer(tmp.trim(),
            SpotSigs.delims);
        while (tokenizer.hasMoreTokens())
          SpotSigs.stopwords.add(tokenizer.nextToken());
      }
      reader.close();

      if (SpotSigs.globalDFs == null)
        SpotSigs.globalDFs = new OpenIntIntHashMap(1000000);

      if (distinctSpots == null)
        distinctSpots = new HashSet<Integer>(1000000);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public class SpotSigsProcessor extends Thread {

    private SpotSigsIndexer indexer;

    private SpotSigs spotSigs;

    public boolean isIndexing, isInverting, isSorting;

    public int id;

    public SpotSigsProcessor(SpotSigsIndexer indexer, int id) throws Exception {
      this.indexer = indexer;
      this.spotSigs = new SpotSigs();
      this.id = id;
    }

    public void run() {
      this.isIndexing = true;
      this.isInverting = true;
      this.isSorting = true;
      try {
        if (MODE.equals("LSH"))
          processLSH();
        else if (MODE.equals("IMATCH"))
          processIMatch();
        else
          processSpotSigs();
      } catch (Exception e) {
        e.printStackTrace();
      }
      synchronized (threads) {
        indexer.threads.remove(this);
      }
    }

    private void processSpotSigs() throws Exception {
      File target;
      InputStream inputStream;

      OpenIntObjectHashMap invertedIndex;
      ArrayList<Counter> indexArray; // variable-length unsorted
      Counter[] indexList; // fixed-length sorted
      IntArrayList keys;
      ArrayList list;
      Counter counter;
      SpotSigs.Partition partition;

      int i, c, key;
      boolean b;
      double idf, normIdf;

      parseTime = Math.min(parseTime, new java.util.Date().getTime());

      this.isIndexing = indexer.fileIterator.hasNext();
      while (indexer.fileIterator.hasNext()) {
        synchronized (indexer.fileIterator) {
          target = indexer.fileIterator.next();
        }

        inputStream = null;
        if (target.toString().toLowerCase().endsWith(".gz"))
          inputStream = new GZIPInputStream(new BufferedInputStream(
              new FileInputStream(target), bufferedReadSize));
        else
          inputStream = new BufferedInputStream(new FileInputStream(target),
              bufferedReadSize);

        if (FORMAT.equals("TREC"))
          spotSigs.createIndexTREC(new BufferedReader(new InputStreamReader(
              inputStream)));
        else if (FORMAT.equals("ARC"))
          spotSigs.createIndexARC(new BufferedReader(new InputStreamReader(
              inputStream)));
        else if (FORMAT.equals("WB"))
          spotSigs.createIndexWB(new BufferedReader(new InputStreamReader(
              inputStream)));
        else
          spotSigs.createIndexSingleFile(target, new BufferedReader(
              new InputStreamReader(inputStream)));

        Thread.yield();
      }
      this.isIndexing = false;

      // Wait for other threads
      do {
        b = isIndexing;
        Thread.yield();
        for (SpotSigsProcessor p : threads) {
          b = b || p.isIndexing;
          if (b)
            break;
        }
      } while (b);

      synchronized (counters1) {
        if (indexIterator1 == null) {
          indexIterator1 = counters1.iterator();
          base = counters1.size();
        }
      }

      // Invert lists and filter index
      filterTime = Math.min(filterTime, new java.util.Date().getTime());

      normIdf = Math.log(SpotSigsIndexer.base + 1);

      while (true) {

        synchronized (indexIterator1) {
          if (!indexIterator1.hasNext())
            break;
          counter = indexIterator1.next();
        }

        synchronized (counter) {

          // Skip overly long (meaningless?) lists by global DF value
          keys = counter.entries.keys();
          for (i = 0; i < keys.size(); i++) {
            key = keys.get(i);
            idf = Math.log((SpotSigsIndexer.base + 1.0)
                / SpotSigs.globalDFs.get(key))
                / normIdf;
            if (idf < SpotSigs.minIdf || idf > SpotSigs.maxIdf) {
              c = counter.getCount(key);
              counter.entries.removeKey(key);
              counter.totalCount -= c;
            } else {
              distinctSpots.add(key);
            }
          }
          SpotSigs.allspots += counter.totalCount;

          // Put into best partition
          if (counter.totalCount >= SpotSigs.minSpotSigs) {
            partition = SpotSigs.getPartition(counter.totalCount);
            keys = counter.entries.keys();
            for (i = 0; i < keys.size(); i++) {
              key = keys.get(i);
              synchronized (partition.unsortedIndex) {
                if ((indexArray = (ArrayList<Counter>) partition.unsortedIndex
                    .get(key)) == null) {
                  indexArray = new ArrayList<Counter>();
                  partition.unsortedIndex.put(key, indexArray);
                }
                indexArray.add(counter);
              }
            }
            synchronized (partition) {
              partition.size++;
              partition.maxLength = Math.max(partition.maxLength,
                  counter.totalCount);
            }
            synchronized (counters2) {
              counters2.add(counter);
            }
          }
        }
      }
      this.isInverting = false;

      // Wait for other threads
      do {
        b = isInverting;
        Thread.yield();
        for (SpotSigsProcessor p : threads) {
          b = b || p.isInverting;
          if (b)
            break;
        }
      } while (b);

      // Sort each index list in descending order of totalLength
      synchronized (SpotSigs.partitions) {
        if (partitions == null) {
          partitions = Arrays.asList(SpotSigs.partitions).iterator();
        }
      }

      invTime = Math.min(invTime, new java.util.Date().getTime());

      while (true) {

        synchronized (partitions) {
          if (!partitions.hasNext())
            break;
          partition = partitions.next();
        }

        synchronized (partition) {
          if (partition.invertedIndex == null) {

            // System.out.println("THREAD " + id + " PARTITION " +
            // partition.idx
            // + " : [" + partition.begin + "<>" + partition.end +
            // "] "
            // + partition.size);

            invertedIndex = new OpenIntObjectHashMap(partition.unsortedIndex
                .size());
            keys = partition.unsortedIndex.keys();
            for (i = 0; i < keys.size(); i++) {
              key = keys.get(i);
              list = (ArrayList) partition.unsortedIndex.get(key);
              indexList = new Counter[list.size()];
              list.toArray(indexList);
              Arrays.sort(indexList);
              invertedIndex.put(key, indexList);
            }

            // Switch to sorted inverted list
            partition.invertedIndex = invertedIndex;
            partition.unsortedIndex = null;
          }
        }
      }
      this.isSorting = false;

      // Wait for other threads
      do {
        b = isSorting;
        Thread.yield();
        for (SpotSigsProcessor p : threads) {
          b = b || p.isSorting;
          if (b)
            break;
        }
      } while (b);

      synchronized (counters2) {
        if (indexIterator2 == null) {
          indexIterator2 = counters2.iterator();
          base = counters2.size();
        }
      }

      // Actual deduplication
      if (id == 0)
        System.out.println("STARTING DEDUPLICATION USING SPOTSIGS: "
            + new java.util.Date() + " [" + numFiles + " FILES, "
            + SpotSigs.partitions.length + " PARTITIONS, "
            + SpotSigs.confidenceThreshold + " CONF, " + SpotSigs.minIdf
            + " minIDF, " + SpotSigs.maxIdf + " maxIDF]");

      dedupTime = Math.min(dedupTime, new java.util.Date().getTime());

      // Start deduplicating the queue
      while (true) {
        synchronized (indexIterator2) {
          if (!indexIterator2.hasNext())
            break;
          counter = indexIterator2.next();
        }
        spotSigs.deduplicateIndex(counter);
        Thread.yield();
      }

      finishTime = Math.max(finishTime, new java.util.Date().getTime());
    }

    private void processLSH() throws Exception {
      File target;
      InputStream inputStream;

      IntArrayList keys;
      Counter counter;

      int j, c, key;
      boolean b;
      double idf, normIdf;

      parseTime = Math.min(parseTime, new java.util.Date().getTime());

      this.isIndexing = indexer.fileIterator.hasNext();
      while (indexer.fileIterator.hasNext()) {
        synchronized (indexer.fileIterator) {
          target = indexer.fileIterator.next();
        }

        inputStream = null;
        if (target.toString().toLowerCase().endsWith(".gz"))
          inputStream = new GZIPInputStream(new BufferedInputStream(
              new FileInputStream(target), bufferedReadSize));
        else
          inputStream = new BufferedInputStream(new FileInputStream(target),
              bufferedReadSize);

        if (FORMAT.equals("TREC"))
          spotSigs.createIndexTREC(new BufferedReader(new InputStreamReader(
              inputStream)));
        else if (FORMAT.equals("ARC"))
          spotSigs.createIndexARC(new BufferedReader(new InputStreamReader(
              inputStream)));
        else if (FORMAT.equals("WB"))
          spotSigs.createIndexWB(new BufferedReader(new InputStreamReader(
              inputStream)));
        else
          spotSigs.createIndexSingleFile(target, new BufferedReader(
              new InputStreamReader(inputStream)));

        Thread.yield();
      }
      this.isIndexing = false;

      // Wait for other threads
      do {
        b = isIndexing;
        Thread.yield();
        for (SpotSigsProcessor p : threads) {
          b = b || p.isIndexing;
          if (b)
            break;
        }
      } while (b);

      filterTime = Math.min(filterTime, new java.util.Date().getTime());

      synchronized (counters1) {
        if (indexIterator1 == null) {
          indexIterator1 = counters1.iterator();
          base = counters1.size();
          lsh = new LSHTable(SpotSigs.k, SpotSigs.l, 1024, SpotSigsIndexer.keys
              .size());
        }
      }

      // Invert and filter index
      normIdf = Math.log(SpotSigsIndexer.base + 1);

      while (true) {

        synchronized (indexIterator1) {
          if (!indexIterator1.hasNext())
            break;
          counter = indexIterator1.next();
        }

        // Skip overly long (meaningless?) lists by global DF value
        synchronized (counter) {
          keys = counter.entries.keys();
          for (j = 0; j < keys.size(); j++) {
            key = keys.get(j);
            idf = Math.log((SpotSigsIndexer.base + 1.0)
                / SpotSigs.globalDFs.get(key))
                / normIdf;
            if (idf < SpotSigs.minIdf || idf > SpotSigs.maxIdf) {
              c = counter.getCount(key);
              counter.entries.removeKey(key);
              counter.totalCount -= c;
            } else {
              distinctSpots.add(key);
            }
          }
        }
        SpotSigs.allspots += counter.totalCount;

        // LSH using multiple Minhash tables
        if (counter.totalCount >= SpotSigs.minSpotSigs) {
          counter.minhashes = new int[SpotSigs.l];
          lsh.put(counter);
          synchronized (counters2) {
            counters2.add(counter);
          }
        }
        Thread.yield();
      }
      this.isInverting = false;

      // Wait for other threads
      do {
        b = isInverting;
        Thread.yield();
        for (SpotSigsProcessor p : threads) {
          b = b || p.isInverting;
          if (b)
            break;
        }
      } while (b);

      synchronized (counters2) {
        if (indexIterator2 == null) {
          indexIterator2 = counters2.iterator();
          base = counters2.size();
        }
      }

      // Actual deduplication
      if (id == 0)
        System.out.println("STARTING DEDUPLICATION USING LSH: "
            + new java.util.Date() + " [k=" + SpotSigs.k + ", l=" + SpotSigs.l
            + ", " + numFiles + " FILES, " + SpotSigs.confidenceThreshold
            + " CONF, " + +SpotSigs.minIdf + " minIDF, " + SpotSigs.maxIdf
            + " maxIDF]");

      dedupTime = Math.min(dedupTime, new java.util.Date().getTime());

      // Start deduplicating the queue
      while (true) {
        synchronized (indexIterator2) {
          if (!indexIterator2.hasNext())
            break;
          counter = indexIterator2.next();
        }
        lsh.deduplicateIndex(counter);
        Thread.yield();
      }

      finishTime = Math.max(finishTime, new java.util.Date().getTime());
    }

    private void processIMatch() throws Exception {
      File target;
      InputStream inputStream;

      IntArrayList keys;
      Counter counter;

      StringBuffer charSequence;
      ArrayList<String> tokenSequence;

      int i, key, tokens;
      boolean b;
      double idf, normIdf;

      Pattern pattern = Pattern.compile("[a-z0-9:]*");
      String token;

      parseTime = Math.min(parseTime, new java.util.Date().getTime());

      this.isIndexing = indexer.fileIterator.hasNext();
      while (indexer.fileIterator.hasNext()) {
        synchronized (indexer.fileIterator) {
          target = indexer.fileIterator.next();
        }
        inputStream = null;
        if (target.toString().toLowerCase().endsWith(".gz"))
          inputStream = new GZIPInputStream(new BufferedInputStream(
              new FileInputStream(target), bufferedReadSize));
        else
          inputStream = new BufferedInputStream(new FileInputStream(target),
              bufferedReadSize);

        if (FORMAT.equals("TREC"))
          spotSigs.createIndexTREC(new BufferedReader(new InputStreamReader(
              inputStream)));
        else if (FORMAT.equals("ARC"))
          spotSigs.createIndexARC(new BufferedReader(new InputStreamReader(
              inputStream)));
        else if (FORMAT.equals("WB"))
          spotSigs.createIndexWB(new BufferedReader(new InputStreamReader(
              inputStream)));
        else
          spotSigs.createIndexSingleFile(target, new BufferedReader(
              new InputStreamReader(inputStream)));

        Thread.yield();
      }
      this.isIndexing = false;

      // Wait for other threads
      do {
        b = isIndexing;
        Thread.yield();
        for (SpotSigsProcessor p : threads) {
          b = b || p.isIndexing;
          if (b)
            break;
        }
      } while (b);

      filterTime = Math.min(filterTime, new java.util.Date().getTime());

      synchronized (counters1) {
        if (indexIterator1 == null) {
          indexIterator1 = counters1.iterator();
          base = counters1.size();
          sha1 = new SHA1Table();
        }
      }

      // Filter tokens and create char sequence for SHA1 hash
      normIdf = Math.log(SpotSigsIndexer.base + 1);
      while (true) {

        synchronized (indexIterator1) {
          if (!indexIterator1.hasNext())
            break;
          counter = indexIterator1.next();
        }

        tokens = 0;
        charSequence = new StringBuffer();
        tokenSequence = new ArrayList<String>(counter.entries.size());

        // Skip overly long (meaningless?) lists by global DF value
        synchronized (counter) {
          keys = counter.entries.keys();
          for (i = 0; i < keys.size(); i++) {
            key = keys.get(i);
            idf = Math.log((SpotSigsIndexer.base + 1.0)
                / SpotSigs.globalDFs.get(key))
                / normIdf;
            if (idf >= SpotSigs.minIdf
                && (FORMAT.equals("TREC") || idf <= SpotSigs.maxIdf)) {
              token = invKeys.get(key);
              if (token != null && pattern.matcher(token).matches()) {
                tokenSequence.add(token);
                distinctSpots.add(key);
                tokens++;
              }
            }
          }
        }
        SpotSigs.allspots += tokens;

        String[] seq = new String[tokenSequence.size()];
        tokenSequence.toArray(seq);
        Arrays.sort(seq);
        for (i = 0; i < seq.length; i++)
          charSequence.append(seq[i]);

        // SHA1-Hash for I-Match
        if (tokens >= SpotSigs.minSpotSigs) {
          // counter.seq = charSequence.toString();
          sha1.put(counter, charSequence.toString());
          synchronized (counters2) {
            counters2.add(counter);
          }
        }
        Thread.yield();
      }
      this.isInverting = false;

      // Wait for other threads
      do {
        b = isInverting;
        Thread.yield();
        for (SpotSigsProcessor p : threads) {
          b = b || p.isInverting;
          if (b)
            break;
        }
      } while (b);

      synchronized (counters2) {
        if (indexIterator2 == null) {
          indexIterator2 = counters2.iterator();
          base = counters2.size();
        }
      }

      // Actual deduplication
      if (id == 0)
        System.out.println("STARTING DEDUPLICATION USING I-MATCH/SHA1: "
            + new java.util.Date() + " [" + numFiles + " FILES, "
            + SpotSigs.confidenceThreshold + " CONF, " + SpotSigs.minIdf
            + " minIDF, " + SpotSigs.maxIdf + " maxIDF]");

      dedupTime = Math.min(dedupTime, new java.util.Date().getTime());

      // Start deduplicating the queue
      while (true) {
        synchronized (indexIterator2) {
          if (!indexIterator2.hasNext())
            break;
          counter = indexIterator2.next();
        }
        sha1.deduplicateIndex(counter);
        Thread.yield();
      }

      finishTime = Math.max(finishTime, new java.util.Date().getTime());
    }
  }
}
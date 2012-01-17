import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedHashSet;

public class SHA1Table extends HashMap<String, LinkedHashSet> {

  private static String convertToHex(byte[] data) {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < data.length; i++) {
      int halfbyte = (data[i] >>> 4) & 0x0F;
      int two_halfs = 0;
      do {
        if ((0 <= halfbyte) && (halfbyte <= 9))
          buf.append((char) ('0' + halfbyte));
        else
          buf.append((char) ('a' + (halfbyte - 10)));
        halfbyte = data[i] & 0x0F;
      } while (two_halfs++ < 1);
    }
    return buf.toString();
  }

  public synchronized static String SHA1(String text)
      throws NoSuchAlgorithmException, UnsupportedEncodingException {
    MessageDigest md = MessageDigest.getInstance("SHA-1");
    byte[] sha1hash = new byte[40];
    try {
      md.update(text.getBytes("iso-8859-1"), 0, text.length());
      sha1hash = md.digest();
    } catch (Exception e) {
      System.err.println(e.getMessage() + " for '" + text + "'");
    }
    return convertToHex(sha1hash);
  }

  public synchronized void put(Counter counter, String content) {
    try {
      counter.SHA1 = SHA1(content);
      LinkedHashSet<Counter> bucket = null;
      if ((bucket = super.get(counter.SHA1)) == null) {
        bucket = new LinkedHashSet<Counter>();
        super.put(counter.SHA1, bucket);
      }
      bucket.add(counter);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void deduplicateIndex(Counter counter) {

    SpotSigsIndexer.incrProc();

    // Get all the colliding docs from a single bucket
    LinkedHashSet<Counter> bucket = super.get(counter.SHA1);

    // Check for near duplicates
    for (Counter counter2 : bucket) {
      double sim = 1.0;
      if (counter != counter2
          && (counter.SHA1.equals(counter2.SHA1) || (sim = LSHTable.getJaccard(
              counter, counter2, SpotSigs.confidenceThreshold)) >= SpotSigs.confidenceThreshold)) {
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
}


package la.common;
import java.util.concurrent.ThreadLocalRandom;

public class ZipfianGenerator{
  public static final double ZIPFIAN_CONSTANT = 0.99;

  /**
   * Number of items.
   */
  private final long items;

  private Number lastVal;

  /**
   * Min item to generate.
   */
  private final long base;

  /**
   * The zipfian constant to use.
   */
  private final double zipfianconstant;

  /**
   * Computed parameters for generating the distribution.
   */
  private double alpha, zetan, eta, theta, zeta2theta;

  /**
   * The number of items used to compute zetan the last time.
   */
  private long countforzeta;

  private boolean allowitemcountdecrease = false;

  /******************************* Constructors **************************************/

  /**
   * Create a zipfian generator for the specified number of items.
   * param items The number of items in the distribution.
   */
  public ZipfianGenerator(long items) {
    this(0, items - 1);
  }

  /**
   * Create a zipfian generator for items between min and max.
   * param min The smallest integer to generate in the sequence.
   * param max The largest integer to generate in the sequence.
   */
  public ZipfianGenerator(long min, long max) {
    this(min, max, ZIPFIAN_CONSTANT);
  }

  /**
   * Create a zipfian generator for the specified number of items using the specified zipfian constant.
   *
   * param items The number of items in the distribution.
   * param zipfianconstant The zipfian constant to use.
   */
  public ZipfianGenerator(long items, double zipfianconstant) {
    this(0, items - 1, zipfianconstant);
  }

  /**
   * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant.
   * param min The smallest integer to generate in the sequence.
   * param max The largest integer to generate in the sequence.
   * param zipfianconstant The zipfian constant to use.
   */
  public ZipfianGenerator(long min, long max, double zipfianconstant) {
    this(min, max, zipfianconstant, zetastatic(max - min + 1, zipfianconstant));
  }

  /**
   * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant, using
   * the precomputed value of zeta.
   *
   * param min The smallest integer to generate in the sequence.
   * param max The largest integer to generate in the sequence.
   * param zipfianconstant The zipfian constant to use.
   * param zetan The precomputed zeta constant.
   */
  public ZipfianGenerator(long min, long max, double zipfianconstant, double zetan) {

    items = max - min + 1;
    base = min;
    this.zipfianconstant = zipfianconstant;

    theta = this.zipfianconstant;

    zeta2theta = zeta(2, theta);
    
    alpha = 1.0 / (1.0 - theta);
    this.zetan = zetan;
    countforzeta = items;
    eta = (1 - Math.pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / this.zetan);

    nextValue();
  }

  /**************************************************************************/

  /**
   * Compute the zeta constant needed for the distribution. Do this from scratch for a distribution with n items,
   * using the zipfian constant thetaVal. Remember the value of n, so if we change the itemcount, we can recompute zeta.
   *
   * param n The number of items to compute zeta over.
   * param thetaVal The zipfian constant.
   */
  double zeta(long n, double thetaVal) {
    countforzeta = n;
    return zetastatic(n, thetaVal);
  }

  /**
   * Compute the zeta constant needed for the distribution. Do this from scratch for a distribution with n items,
   * using the zipfian constant theta. This is a static version of the function which will not remember n.
   * param n The number of items to compute zeta over.
   * param theta The zipfian constant.
   */
  static double zetastatic(long n, double theta) {
    return zetastatic(0, n, theta, 0);
  }

  /**
   * Compute the zeta constant needed for the distribution. Do this incrementally for a distribution that
   * has n items now but used to have st items. Use the zipfian constant thetaVal. Remember the new value of
   * n so that if we change the itemcount, we'll know to recompute zeta.
   *
   * param st The number of items used to compute the last initialsum
   * param n The number of items to compute zeta over.
   * param thetaVal The zipfian constant.
   * param initialsum The value of zeta we are computing incrementally from.
   */
  double zeta(long st, long n, double thetaVal, double initialsum) {
    countforzeta = n;
    return zetastatic(st, n, thetaVal, initialsum);
  }

  /**
   * Compute the zeta constant needed for the distribution. Do this incrementally for a distribution that
   * has n items now but used to have st items. Use the zipfian constant theta. Remember the new value of
   * n so that if we change the itemcount, we'll know to recompute zeta.
   * param st The number of items used to compute the last initialsum
   * param n The number of items to compute zeta over.
   * param theta The zipfian constant.
   * param initialsum The value of zeta we are computing incrementally from.
   */
  static double zetastatic(long st, long n, double theta, double initialsum) {
    double sum = initialsum;
    for (long i = st; i < n; i++) {

      sum += 1 / (Math.pow(i + 1, theta));
    }

    //System.out.println("countforzeta="+countforzeta);

    return sum;
  }

  /****************************************************************************************/


  /**
   * Generate the next item as a long.
   *
   * param itemcount The number of items in the distribution.
   * return The next item in the sequence.
   */
  long nextLong(long itemcount) {
    //from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994

    if (itemcount != countforzeta) {

      //have to recompute zetan and eta, since they depend on itemcount
      synchronized (this) {
        if (itemcount > countforzeta) {
          //System.err.println("WARNING: Incrementally recomputing Zipfian distribtion. (itemcount="+itemcount+"
          // countforzeta="+countforzeta+")");

          //we have added more items. can compute zetan incrementally, which is cheaper
          zetan = zeta(countforzeta, itemcount, theta, zetan);
          eta = (1 - Math.pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);
        } else if ((itemcount < countforzeta) && (allowitemcountdecrease)) {
          //have to start over with zetan
          //note : for large itemsets, this is very slow. so don't do it!

          //TODO: can also have a negative incremental computation, e.g. if you decrease the number of items,
          // then just subtract the zeta sequence terms for the items that went away. This would be faster than
          // recomputing from scratch when the number of items decreases

          System.err.println("WARNING: Recomputing Zipfian distribtion. This is slow and should be avoided. " +
              "(itemcount=" + itemcount + " countforzeta=" + countforzeta + ")");

          zetan = zeta(itemcount, theta);
          eta = (1 - Math.pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);
        }
      }
    }

    double u = ThreadLocalRandom.current().nextDouble();
    double uz = u * zetan;

    if (uz < 1.0) {
      return base;
    }

    if (uz < 1.0 + Math.pow(0.5, theta)) {
      return base + 1;
    }

    long ret = base + (long) ((itemcount) * Math.pow(eta * u - eta + 1, alpha));
    setLastValue(ret);
    return ret;
  }


  public void setLastValue(Number last) {
  	this.lastVal = last;
  }

  /**
   * Return the next value, skewed by the Zipfian distribution. The 0th item will be the most popular, followed by
   * the 1st, followed by the 2nd, etc. (Or, if min != 0, the min-th item is the most popular, the min+1th item the
   * next most popular, etc.) If you want the popular items scattered throughout the item space, use
   * ScrambledZipfianGenerator instead.
   */
  
  public Long nextValue() {
    return nextLong(items);
  }

  /**
   * todo Implement ZipfianGenerator.mean()
   */
  
  public double mean() {
    throw new UnsupportedOperationException("todo implement ZipfianGenerator.mean()");
  }
}

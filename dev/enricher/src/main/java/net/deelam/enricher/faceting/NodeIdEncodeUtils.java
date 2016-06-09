package net.deelam.enricher.faceting;

public class NodeIdEncodeUtils {

  static long concat(int a, int b) {
    return (long) a << 32 | b & 0xFFFFFFFFL;
  }

  static int getFirstInt(long n) {
    return (int) (n >> 32);
  }

  static int getSecondInt(long c) {
    return (int) c;
  }

  public static void main(String[] args) {
    long c = 158913789990L;
    System.out.println(getFirstInt(c)+" "+getSecondInt(c));
    
    
    check(Integer.MIN_VALUE, Integer.MIN_VALUE);
    check(Integer.MIN_VALUE, Integer.MAX_VALUE);
    check(Integer.MAX_VALUE, Integer.MAX_VALUE);
    check(Integer.MAX_VALUE, Integer.MIN_VALUE);
    check(0, 0);

    int segments = 20_000; // 100000 takes less than 1 min to finish
    int INCR = Integer.MAX_VALUE / segments;
    System.out.println(Integer.MAX_VALUE + " " + INCR);
    int a = Integer.MIN_VALUE;
    for (int i = 0; i <= segments * 2; ++i, a += INCR) {
      //System.out.println(a);
      int b = Integer.MIN_VALUE;
      for (int j = 0; j <= segments * 2; ++j, b += INCR) {
        //System.out.println(a+" "+b);
        check(a, b);
      }
    }

    //    System.out.println(Integer.toBinaryString(a));
    //    System.out.println(Long.toBinaryString(a));
    //    System.out.println(Long.toBinaryString(a << 32));
    //    System.out.println(Long.toBinaryString(b & 0xFFFFFFFFL));
    //    System.out.println(Long.toBinaryString(c));
    //    System.out.println(a+" "+b+" "+c);
    //    System.out.println(aBack+" "+bBack);
  }

  static void check(int a, int b) {
    long c = concat(a, b);
    int aBack = getFirstInt(c);
    int bBack = getSecondInt(c);
    if (aBack != a || bBack != b)
      System.err.println("!!! " + a + " " + b);
  }

}

package net.deelam.graphtools.util;

import static com.google.common.base.Preconditions.checkArgument;

public class IdUtils {

  public static String convertToSafeChars(String string, int maxLength) {
    checkArgument(maxLength > 4);
    int length = Math.min(string.length(), maxLength);
    int midIndex = length;
    if (string.length() > maxLength)
      midIndex = (length / 2) - 2;
  
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < string.length(); ++i) {
      if (i == midIndex) {
        sb.append("__");
        i = string.length() - (midIndex + 2); //jump
      }
      char c = string.charAt(i);
      if ( (c >= 65 && c <= 90) // uppercase letter
          || (c >= 97 && c <= 122) // lowercase letter
          || (c >= 48 && c <= 57) // number
          || ((c == 45 || c == 46 || c == 95) && sb.length()>0 ) // -,.,_ but not as first character
          )
        sb.append(c);
      else if(sb.length()>0) // don't use '-' as a first character
        sb.append('_');
    }
    return sb.toString();
  }

}

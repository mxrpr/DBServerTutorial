package com.mixer.util;

import java.util.stream.IntStream;

public final class Leveinshtein {

    public static int leveinshteinDistance(final String lhs, final String rhs) {
        if (lhs == null || rhs == null ) return -1;
        if (lhs.equalsIgnoreCase(rhs)) return 0;
        if (lhs.isEmpty()) return rhs.length();
        if (rhs.isEmpty()) return lhs.length();

        int[][] result = new int[lhs.length()][rhs.length()];

        // initialize
        IntStream.range(0, lhs.length()).forEach(i-> result[i][0] = i);

        IntStream.range(0, rhs.length()).forEach(i-> result[0][i] = i);

        int subst;
        for (int j=1; j < rhs.length(); j++) {
            for(int i=1; i < lhs.length(); i++) {
                if (lhs.charAt(i-1) == rhs.charAt(j-1))
                    subst = 0;
                else
                    subst = 1;
                int deletion = result[i-1][j] + 1;
                int insertion = result[i][j-1] + 1;
                int substitution = result[i-1][j-1] + subst;
                result[i][j] = Math.min(Math.min(deletion, insertion), substitution);
            }
        }

        return result[lhs.length()-1][rhs.length()-1];
    }
}

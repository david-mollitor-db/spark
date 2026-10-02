/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.unsafe.array;

import java.util.Arrays;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Tests whether a byte sequence contains any of a fixed set of needles, in one pass over the
 * input no matter how many needles there are.
 * <p>
 * This is an Aho-Corasick automaton compiled into a DFA over bytes: every state has a
 * precomputed transition for every input byte, so the scan does one table lookup per input byte
 * and stops at the first byte that completes any needle. Matching is on raw bytes, so for UTF-8
 * input the result is the same as calling {@link UTF8String#contains} once per needle.
 * <p>
 * Bytes that occur in no needle all behave the same, so they share one column of the table.
 * The table therefore has {@code numStates * numClasses} entries, where numStates is at most one
 * more than the total length of the needles and numClasses at most one more than the number of
 * distinct bytes in the needles; see {@link #estimateTableBytes}.
 * <p>
 * Instances are immutable and safe to share between threads.
 */
public final class MultiSubstringMatcher {

  // Transition target meaning "a needle ends here"; the scan stops as soon as it sees one.
  private static final int MATCH = -1;

  // Column of the transition table for each byte value. Column 0 is shared by all bytes that
  // occur in no needle.
  private final int[] byteClass;
  // Row-major transition table. An entry is the next state already multiplied by the row width,
  // so it can be added to a column directly, or MATCH.
  private final int[] transitions;
  // An empty needle is contained in every input.
  private final boolean matchesEverything;

  public MultiSubstringMatcher(byte[][] needles) {
    int[] byteClass = new int[256];
    int numClasses = 1;
    long totalNeedleBytes = 0;
    for (byte[] needle : needles) {
      totalNeedleBytes += needle.length;
      for (byte b : needle) {
        if (byteClass[b & 0xFF] == 0) {
          byteClass[b & 0xFF] = numClasses++;
        }
      }
    }
    long maxEntries = (totalNeedleBytes + 1) * numClasses;
    if (maxEntries > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
      throw new IllegalArgumentException(
        "Needles are too large for a MultiSubstringMatcher: " + totalNeedleBytes + " bytes");
    }

    // Build the trie of the needles directly in the transition table. 0 means "no child" here:
    // the root is state 0 and no trie edge leads back to it.
    int[] next = new int[(int) maxEntries];
    boolean[] accepting = new boolean[(int) totalNeedleBytes + 1];
    int numStates = 1;
    for (byte[] needle : needles) {
      int state = 0;
      for (byte b : needle) {
        int idx = state * numClasses + byteClass[b & 0xFF];
        if (next[idx] == 0) {
          next[idx] = numStates++;
        }
        state = next[idx];
      }
      accepting[state] = true;
    }
    this.matchesEverything = accepting[0];

    // Turn the trie into a DFA, breadth first. A missing edge of state s takes the same edge from
    // fail(s), the state for the longest proper suffix of s that is also in the trie. fail(s) is
    // shallower than s, so its row is already complete when s is processed. The root's missing
    // edges stay 0, back to the root.
    int[] fail = new int[numStates];
    int[] queue = new int[numStates];
    int head = 0;
    int tail = 0;
    for (int c = 0; c < numClasses; c++) {
      if (next[c] != 0) {
        queue[tail++] = next[c];
      }
    }
    while (head < tail) {
      int state = queue[head++];
      // A needle also ends here if one ends at the suffix state.
      accepting[state] |= accepting[fail[state]];
      int row = state * numClasses;
      int failRow = fail[state] * numClasses;
      for (int c = 0; c < numClasses; c++) {
        int child = next[row + c];
        if (child != 0) {
          fail[child] = next[failRow + c];
          queue[tail++] = child;
        } else {
          next[row + c] = next[failRow + c];
        }
      }
    }

    int[] transitions = Arrays.copyOf(next, numStates * numClasses);
    for (int i = 0; i < transitions.length; i++) {
      int target = transitions[i];
      transitions[i] = accepting[target] ? MATCH : target * numClasses;
    }
    this.byteClass = byteClass;
    this.transitions = transitions;
  }

  /**
   * Returns an upper bound on the size in bytes of the transition table for these needles,
   * without building it.
   */
  public static long estimateTableBytes(byte[][] needles) {
    boolean[] seen = new boolean[256];
    long numClasses = 1;
    long totalNeedleBytes = 0;
    for (byte[] needle : needles) {
      totalNeedleBytes += needle.length;
      for (byte b : needle) {
        if (!seen[b & 0xFF]) {
          seen[b & 0xFF] = true;
          numClasses++;
        }
      }
    }
    return (totalNeedleBytes + 1) * numClasses * Integer.BYTES;
  }

  /**
   * Returns true if the {@code numBytes} bytes at {@code offset} in {@code base} contain any of
   * the needles. {@code base} and {@code offset} are interpreted as by {@link Platform#getByte}.
   */
  public boolean matches(Object base, long offset, int numBytes) {
    if (matchesEverything) {
      return true;
    }
    final int[] byteClass = this.byteClass;
    final int[] transitions = this.transitions;
    int state = 0;
    for (int i = 0; i < numBytes; i++) {
      state = transitions[state + byteClass[Platform.getByte(base, offset + i) & 0xFF]];
      if (state < 0) {
        return true;
      }
    }
    return false;
  }

  /** Returns true if {@code s} contains any of the needles. */
  public boolean matches(UTF8String s) {
    return matches(s.getBaseObject(), s.getBaseOffset(), s.numBytes());
  }
}

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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MultiSubstringMatcherSuite {

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static MultiSubstringMatcher matcher(String... needles) {
    byte[][] bytes = new byte[needles.length][];
    for (int i = 0; i < needles.length; i++) {
      bytes[i] = utf8(needles[i]);
    }
    return new MultiSubstringMatcher(bytes);
  }

  private static boolean matches(MultiSubstringMatcher m, byte[] input) {
    return m.matches(input, Platform.BYTE_ARRAY_OFFSET, input.length);
  }

  private static void assertMatches(MultiSubstringMatcher m, String input) {
    Assertions.assertTrue(m.matches(UTF8String.fromString(input)), input);
  }

  private static void assertNoMatch(MultiSubstringMatcher m, String input) {
    Assertions.assertFalse(m.matches(UTF8String.fromString(input)), input);
  }

  @Test
  public void noNeedles() {
    MultiSubstringMatcher m = matcher();
    assertNoMatch(m, "");
    assertNoMatch(m, "abc");
  }

  @Test
  public void emptyNeedleMatchesEverything() {
    MultiSubstringMatcher m = matcher("xyz", "");
    assertMatches(m, "");
    assertMatches(m, "abc");
  }

  @Test
  public void singleNeedle() {
    MultiSubstringMatcher m = matcher("abc");
    assertMatches(m, "abc");
    assertMatches(m, "xxabcxx");
    assertMatches(m, "aabc");
    assertMatches(m, "ababc");
    assertNoMatch(m, "");
    assertNoMatch(m, "ab");
    assertNoMatch(m, "abd");
    assertNoMatch(m, "acb");
  }

  @Test
  public void matchAtStartAndEnd() {
    MultiSubstringMatcher m = matcher("sta", "end");
    assertMatches(m, "start");
    assertMatches(m, "the end");
    assertNoMatch(m, "en");
    assertNoMatch(m, "st");
  }

  @Test
  public void needleLongerThanInput() {
    MultiSubstringMatcher m = matcher("abcdef");
    assertNoMatch(m, "abc");
    assertNoMatch(m, "abcde");
    assertMatches(m, "abcdef");
  }

  @Test
  public void overlappingNeedles() {
    MultiSubstringMatcher m = matcher("he", "she", "his", "hers");
    assertMatches(m, "ushers");
    assertMatches(m, "ahis");
    assertNoMatch(m, "h");
    assertNoMatch(m, "hi");
    assertNoMatch(m, "sh");

    MultiSubstringMatcher abab = matcher("abab", "bab");
    assertMatches(abab, "abab");
    assertMatches(abab, "xbabx");
    assertNoMatch(abab, "aba");
    assertNoMatch(abab, "abaab");
  }

  @Test
  public void needleFoundOnlyThroughSuffix() {
    // In "abce", the scan is inside "abcd" when "bc" ends.
    assertMatches(matcher("abcd", "bc"), "abce");
    // In "abcf", the scan has to leave "abcd" for "bcf" without starting over.
    assertMatches(matcher("abcd", "bcf"), "abcf");
    assertNoMatch(matcher("abcd", "bcf"), "abce");
  }

  @Test
  public void duplicateAndPrefixNeedles() {
    MultiSubstringMatcher m = matcher("abc", "abc", "ab", "abcd");
    assertMatches(m, "xab");
    assertMatches(m, "abcd");
    assertNoMatch(m, "xa");
    assertNoMatch(m, "axb");
  }

  @Test
  public void multiByteAndHighBytes() {
    // U+00E9 and U+65E5 U+672C as UTF-8, written as bytes to keep this file ASCII.
    byte[] eAcute = {(byte) 0xC3, (byte) 0xA9};
    byte[] nihon = {(byte) 0xE6, (byte) 0x97, (byte) 0xA5, (byte) 0xE6, (byte) 0x9C, (byte) 0xAC};
    MultiSubstringMatcher m = new MultiSubstringMatcher(new byte[][] {eAcute, nihon});
    Assertions.assertTrue(matches(m, new byte[] {'c', 'a', 'f', (byte) 0xC3, (byte) 0xA9}));
    Assertions.assertFalse(matches(m, new byte[] {'c', 'a', 'f', (byte) 0xC3, (byte) 0xA8}));
    Assertions.assertTrue(matches(m, Arrays.copyOf(nihon, nihon.length)));
    Assertions.assertFalse(matches(m, Arrays.copyOf(nihon, 3)));

    MultiSubstringMatcher raw = new MultiSubstringMatcher(new byte[][] {{0x00, (byte) 0xFF}});
    Assertions.assertTrue(matches(raw, new byte[] {1, 0x00, (byte) 0xFF, 2}));
    Assertions.assertFalse(matches(raw, new byte[] {(byte) 0xFF, 0x00}));
  }

  @Test
  public void offsetsAndOffHeapInput() {
    MultiSubstringMatcher m = matcher("needle");
    byte[] bytes = utf8("xxneedleyy");
    Assertions.assertTrue(m.matches(bytes, Platform.BYTE_ARRAY_OFFSET + 2, 6));
    Assertions.assertFalse(m.matches(bytes, Platform.BYTE_ARRAY_OFFSET + 2, 5));
    Assertions.assertFalse(m.matches(bytes, Platform.BYTE_ARRAY_OFFSET + 3, 7));

    long address = Platform.allocateMemory(bytes.length);
    try {
      Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, null, address, bytes.length);
      Assertions.assertTrue(m.matches(null, address, bytes.length));
      Assertions.assertFalse(m.matches(null, address + 3, bytes.length - 3));
    } finally {
      Platform.freeMemory(address);
    }
  }

  @Test
  public void agreesWithContainsOnRandomInputs() {
    // A small alphabet, including bytes >= 0x80, so that needles often occur and overlap.
    byte[] alphabet = {'a', 'b', 'c', (byte) 0xC3, (byte) 0xFF};
    Random random = new Random(42);
    for (int round = 0; round < 2000; round++) {
      byte[][] needles = new byte[random.nextInt(6)][];
      for (int i = 0; i < needles.length; i++) {
        int length = random.nextInt(10) == 0 ? 0 : 1 + random.nextInt(4);
        needles[i] = randomBytes(random, alphabet, length);
      }
      MultiSubstringMatcher m = new MultiSubstringMatcher(needles);
      for (int j = 0; j < 20; j++) {
        byte[] input = randomBytes(random, alphabet, random.nextInt(12));
        boolean expected = false;
        for (byte[] needle : needles) {
          expected |= ByteArrayMethods.contains(input, needle);
        }
        Assertions.assertEquals(expected, matches(m, input),
          () -> "needles " + Arrays.deepToString(needles) + " input " + Arrays.toString(input));
      }
    }
  }

  private static byte[] randomBytes(Random random, byte[] alphabet, int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = alphabet[random.nextInt(alphabet.length)];
    }
    return bytes;
  }

  @Test
  public void estimateTableBytes() {
    // 5 needle bytes over 3 distinct values: at most 6 states, 4 columns, 4 bytes per entry.
    Assertions.assertEquals(6 * 4 * 4,
      MultiSubstringMatcher.estimateTableBytes(new byte[][] {utf8("ab"), utf8("abc")}));
    Assertions.assertEquals(4, MultiSubstringMatcher.estimateTableBytes(new byte[0][]));
  }
}

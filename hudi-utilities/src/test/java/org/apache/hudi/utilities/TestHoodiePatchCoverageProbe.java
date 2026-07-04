/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Covers every branch of {@link PatchCoverageProbe} so the patch-coverage gate
 * sees ~100% coverage of the changed lines and passes.
 */
public class TestHoodiePatchCoverageProbe {

  @Test
  public void testClassify() {
    assertEquals("positive", PatchCoverageProbe.classify(5));
    assertEquals("negative", PatchCoverageProbe.classify(-5));
    assertEquals("zero", PatchCoverageProbe.classify(0));
  }

  @Test
  public void testClampToNonNegative() {
    assertEquals(0, PatchCoverageProbe.clampToNonNegative(-3));
    assertEquals(7, PatchCoverageProbe.clampToNonNegative(7));
  }
}

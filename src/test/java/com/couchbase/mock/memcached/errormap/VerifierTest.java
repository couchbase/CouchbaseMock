/*
 * Copyright 2017 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.couchbase.mock.memcached.errormap;

import com.couchbase.mock.memcached.MemcachedServer;
import com.couchbase.mock.memcached.protocol.ErrorCode;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mnunberg on 4/13/17.
 */
public class VerifierTest extends TestCase {
  public void testConstant() throws Exception {
    // Get the spec
    ErrorMap mm = ErrorMap.DEFAULT_ERRMAP;
    int opcode = ErrorCode.DUMMY_RETRY_CONSTANT.value();

    // Fill the log
    List<MemcachedServer.CommandLogEntry> entries = new ArrayList<MemcachedServer.CommandLogEntry>();
    // Fill up the entries with a simple timestamp..

    for (int i = 0; i < 10; i++) {
      entries.add(new MemcachedServer.CommandLogEntry(opcode, (long)i));
    }

    // Verify the spec!
    ErrorMapEntry mmEntry = mm.getErrorEntry(opcode);
    assertNotNull(mmEntry);
    RetrySpec spec = mmEntry.getRetrySpec();

    assertFalse(Verifier.verify(entries, spec, opcode));

    // Test when we're all according to spec.
    entries.clear();

    // Represents the first error received..
    entries.add(new MemcachedServer.CommandLogEntry(opcode, 0));

    int curTime = spec.getAfter();
    while (curTime < spec.getMaxDuration() + spec.getAfter()) {
      entries.add(new MemcachedServer.CommandLogEntry(opcode, curTime));
      curTime += spec.getInterval();
    }

    Verifier.verifyThrow(entries, spec, opcode);
  }

  public void testLinear() throws Exception {
    ErrorMap mm = ErrorMap.DEFAULT_ERRMAP;
    int opcode = ErrorCode.DUMMY_RETRY_LINEAR.value();
    ErrorMapEntry mmEntry = mm.getErrorEntry(opcode);
    assertNotNull(mmEntry);
    RetrySpec spec = mmEntry.getRetrySpec();
    List<MemcachedServer.CommandLogEntry> entries = new ArrayList<MemcachedServer.CommandLogEntry>();

    // Verify with an empty command set
    assertFalse(Verifier.verify(entries, spec, opcode));

    for (int i = 0; i < 10; i++) {
      entries.add(new MemcachedServer.CommandLogEntry(opcode, i));
    }

    assertFalse(Verifier.verify(entries, spec, opcode));
    entries.clear();

    // Try to actually do a "correct" retry
    entries.add(new MemcachedServer.CommandLogEntry(opcode, 0));

    int curTime = spec.getAfter();
    int numAttempts = 0;
    while (curTime < spec.getMaxDuration() + spec.getAfter()) {
//      System.err.printf("Adding spec at %d\n", curTime);
      entries.add(new MemcachedServer.CommandLogEntry(opcode, curTime));
      numAttempts ++;
      int incrBy = numAttempts * spec.getInterval();
      if (spec.getCeil() > 0) {
        incrBy = Math.min(incrBy, spec.getCeil());
      }
//      System.err.printf("Waiting %d\n", incrBy);
      curTime += incrBy;
    }
    Verifier.verifyThrow(entries, spec, opcode);
  }

  public void testExponential() throws Exception {
    ErrorMap mm = ErrorMap.DEFAULT_ERRMAP;
    int opcode = ErrorCode.DUMMY_RETRY_EXPONENTIAL.value();
    ErrorMapEntry mmEntry = mm.getErrorEntry(opcode);
    assertNotNull(mmEntry);
    RetrySpec spec = mmEntry.getRetrySpec();
    assertNotNull(spec);
    List<MemcachedServer.CommandLogEntry> entries = new ArrayList<MemcachedServer.CommandLogEntry>();

    // Test with empty list. Should fail
    assertFalse(Verifier.verify(entries, spec, opcode));

    // Test with multiple entries (without proper waiting). Should fail
    for (int i = 0; i < 10; i++) {
      entries.add(new MemcachedServer.CommandLogEntry(opcode, i));
    }

    assertFalse(Verifier.verify(entries, spec, opcode));

    entries.clear();

    int numAttempts = 0;

    entries.add(new MemcachedServer.CommandLogEntry(opcode, 0));
    int curTime = spec.getAfter();

    while (curTime < spec.getAfter() + spec.getMaxDuration()) {
      entries.add(new MemcachedServer.CommandLogEntry(opcode, curTime));
      numAttempts++;
      int incrBy = (int)Math.pow(spec.getInterval(), numAttempts);
      if (spec.getCeil() > 0) {
        incrBy = Math.min(spec.getCeil(), incrBy);
      }
      curTime += incrBy;
    }

    Verifier.verifyThrow(entries, spec, opcode);
  }
}
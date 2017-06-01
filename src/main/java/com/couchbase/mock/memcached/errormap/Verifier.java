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

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mnunberg on 4/12/17.
 */
public abstract class Verifier {
  public static final String STRATEGY_EXPONENTIAL = "exponential";
  public static final String STRATEGY_LINEAR = "linear";
  public static final String STRATEGY_CONSTANT = "constant";

  // How much "fuzz" to allow in testing, when considering any slowness in the mock.
  public static final long DEFAULT_FUZZ_MS = 10;

  protected MemcachedServer.CommandLogEntry firstEntry = null;

  protected long fuzzMillis = DEFAULT_FUZZ_MS;

  public static class VerificationException extends Exception {
    public VerificationException(String message) {
      super(message);
    }
  }

  static class FirstRetryTooSoonException extends VerificationException {
    public FirstRetryTooSoonException() {
      super("Initial retry does not honor `after`");
    }
  }

  static class DurationExceededException extends VerificationException {
    public DurationExceededException(long expected, long actual) {
      super("Retries lasted for too long (`max-duration`). Expected: " + expected + ". Actual: " + actual);
    }
  }

  protected void verify(List<MemcachedServer.CommandLogEntry> entries, RetrySpec spec) throws VerificationException {
    if (entries.size() < 2) {
      throw new VerificationException("No commands executed: Log has " + entries.size());
    }

    firstEntry = entries.get(0);
    entries.remove(0);

    long beginTime = entries.get(0).getMsTimestamp();
    long endTime = entries.get(entries.size()-1).getMsTimestamp();
    long duration = endTime - beginTime;

    if (duration > spec.getMaxDuration() + fuzzMillis) {
      throw new DurationExceededException(spec.getMaxDuration(), duration);
    }

    if (entries.get(0).getMsTimestamp() - firstEntry.getMsTimestamp() < spec.getAfter() - 1) {
      throw new FirstRetryTooSoonException();
    }
    verifyImpl(entries, spec);
  }

  abstract protected void verifyImpl(List<MemcachedServer.CommandLogEntry> entries, RetrySpec spec) throws VerificationException;

  public static class ConstantVerifier extends Verifier {
    @Override
     protected void verifyImpl(List<MemcachedServer.CommandLogEntry> entries, RetrySpec spec) throws VerificationException {
      for (int i = 1; i < entries.size(); i++) {
        long duration = entries.get(i).getMsTimestamp() - entries.get(i-1).getMsTimestamp();
        if (Math.abs(duration - spec.getInterval()) > fuzzMillis) {
          throw new VerificationException("Too much spacing between intervals: " + duration + ". Expected: " + spec.getInterval());
        }
      }

      // Determine when our *last* retry attempt is supposed to be. This is to ensure
      // that we're not skimping on retries.
      long lastRetryExpected = entries.get(0).getMsTimestamp() + spec.getMaxDuration();

      // We should tolerate the client skipping the last beat
      long lastIntervalMaxDiff = fuzzMillis;

      long lastTimestamp = entries.get(entries.size() - 1).getMsTimestamp();

      if (Math.abs(lastTimestamp - lastRetryExpected) > lastIntervalMaxDiff) {
        throw new VerificationException(
                String.format("Not enough/too many retries. Last TS=%d. Last expected=%d. Diff=%d. MaxDiff=%d",
                        lastTimestamp, lastRetryExpected, Math.abs(lastRetryExpected - lastTimestamp),
                        lastIntervalMaxDiff));
      }
    }
  }

  public static class LinearVerifier extends Verifier {
    @Override
    protected void verifyImpl(List<MemcachedServer.CommandLogEntry> entries, RetrySpec spec) throws VerificationException{
      // Iterate through each log entry.
      for (int i = 1; i < entries.size(); i++) {
        long duration = entries.get(i).getMsTimestamp() - entries.get(i-1).getMsTimestamp();
        long expectedDuration = spec.getInterval() * i;
        expectedDuration = Math.min(spec.getCeil(), expectedDuration);
        if (Math.abs(duration - expectedDuration) > fuzzMillis) {
          throw new VerificationException("Linear backoff failed!. " + " duration: " + duration + ", expected: " + expectedDuration);
        }
      }
      // TODO: I don't know how to calculate the last expected retry
    }
  }

  public static class ExponentialVerifier extends Verifier {
    @Override
    protected void verifyImpl(List<MemcachedServer.CommandLogEntry> entries, RetrySpec spec) throws VerificationException {
      for (int i = 1; i < entries.size(); i++) {
        long duration = entries.get(i).getMsTimestamp() - entries.get(i-1).getMsTimestamp();
        long expectedDuration = (long)Math.pow((double)spec.getInterval(), (double)i);
        if (spec.getCeil() > 0) {
          expectedDuration = Math.min(spec.getCeil(), expectedDuration);
        }
        if (Math.abs(duration - expectedDuration) > fuzzMillis) {
          throw new VerificationException("Exponential backoff failed. Duration: " + duration + ", expected: " + expectedDuration);
        }
      }
    }
  }

  public static void verifyThrow(List<MemcachedServer.CommandLogEntry> allEntries, RetrySpec spec, int opcode) throws VerificationException {
    verifyThrow(allEntries, spec, opcode, 0);
  }

  public static void verifyThrow(List<MemcachedServer.CommandLogEntry> allEntries, RetrySpec spec, int opcode, long fuzzMillis) throws VerificationException {
    List<MemcachedServer.CommandLogEntry> entries = new ArrayList<MemcachedServer.CommandLogEntry>();
    for (MemcachedServer.CommandLogEntry entry : allEntries) {
      if (entry.getOpcode() == opcode) {
        entries.add(entry);
      }
    }

    Verifier verifier;
    if (spec.getStrategy().equals(STRATEGY_CONSTANT)) {
      verifier = new ConstantVerifier();
    } else if (spec.getStrategy().equals(STRATEGY_LINEAR)) {
      verifier = new LinearVerifier();
    } else if (spec.getStrategy().equals(STRATEGY_EXPONENTIAL)) {
      verifier = new ExponentialVerifier();
    } else {
      throw new RuntimeException("No such verifier strategy!");
    }

    if (fuzzMillis > 0) {
      verifier.fuzzMillis = fuzzMillis;
    }

    verifier.verify(entries, spec);

  }

  public static boolean verify(List<MemcachedServer.CommandLogEntry> entries, RetrySpec spec, int opcode) {
    try {
      verifyThrow(entries, spec, opcode);
      return true;
    } catch (VerificationException ex) {
      return false;
    }
  }
}

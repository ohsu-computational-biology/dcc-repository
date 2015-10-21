package org.icgc.dcc.repository.collab.reader;

import org.junit.Ignore;
import org.junit.Test;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CollabS3BucketReaderTest {

  @Test
  @Ignore("For development only")
  public void testReadBucket() {
    val reader = new CollabS3BucketReader();
    for (val summary : reader.readSummaries()) {
      log.info("{}", summary);
    }
  }

}

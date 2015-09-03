package org.icgc.dcc.repository.aws;

import static org.icgc.dcc.repository.core.util.RepositoryFileContexts.newLocalRepositoryFileContext;

import org.junit.Ignore;
import org.junit.Test;

import lombok.val;

public class AWSImporterTest {

  @Test
  @Ignore("For development only")
  public void testExecute() {
    val context = newLocalRepositoryFileContext();
    val awsImporter = new AWSImporter(context);
    awsImporter.execute();
  }

}

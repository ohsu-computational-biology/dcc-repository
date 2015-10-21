package org.icgc.dcc.repository.collab;

import static org.icgc.dcc.repository.core.util.RepositoryFileContexts.newLocalRepositoryFileContext;

import org.junit.Test;

import lombok.val;

public class CollabImporterTest {

  @Test
  // @Ignore("For development only")
  public void testExecute() {
    val context = newLocalRepositoryFileContext();
    val collabImporter = new CollabImporter(context);
    collabImporter.execute();
  }

}

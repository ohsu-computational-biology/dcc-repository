package org.icgc.dcc.repository.collab;

import static org.icgc.dcc.repository.core.util.RepositoryFileContexts.newLocalRepositoryFileContext;

import org.junit.Ignore;
import org.junit.Test;

import lombok.val;

@Ignore("For development only")
public class CollabImporterTest {

  @Test
  public void testExecute() {
    val context = newLocalRepositoryFileContext();
    val collabImporter = new CollabImporter(context);
    collabImporter.execute();
  }

}

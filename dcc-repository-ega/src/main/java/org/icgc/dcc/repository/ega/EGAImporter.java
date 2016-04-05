/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.repository.ega;

import static org.icgc.dcc.repository.core.model.RepositorySource.EGA;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

import org.icgc.dcc.common.core.json.Jackson;
import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.util.GenericRepositorySourceFileImporter;
import org.icgc.dcc.repository.ega.model.EGAMetadataArchive;
import org.icgc.dcc.repository.ega.reader.EGAMetadataArchiveReader;
import org.icgc.dcc.repository.ega.util.EGAClient;
import org.icgc.dcc.repository.ega.util.EGAProjects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * @see https://www.ebi.ac.uk/ega/dacs/EGAC00001000010
 */
@Slf4j
public class EGAImporter extends GenericRepositorySourceFileImporter {

  public EGAImporter(RepositoryFileContext context) {
    super(EGA, context, log);
  }

  @Override
  @SneakyThrows
  protected Iterable<RepositoryFile> readFiles() {
    val reader = new EGAMetadataArchiveReader();
    val client = createEGAClient();
    val datasetIds = client.getDatasetIds();

    @Cleanup
    val writer = new FileWriter(new File(new File(System.getProperty("user.home")), "icgc-ega-datasets2.jsonl"));

    val mapper = Jackson.DEFAULT.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

    int i = 0;
    val watch = Stopwatch.createStarted();
    for (val datasetId : datasetIds) {
      i++;

      val metadata = reader.read(datasetId);
      val files = client.getFiles(datasetId);
      val projectCodes = EGAProjects.getDatasetProjectCodes(datasetId);

      log.info("{}. {} = {}", i, metadata.getDatasetId(), projectCodes);
      val record = new EGAMetadata(datasetId, projectCodes, files, metadata);

      mapper.writeValue(writer, record);
      writer.write("\n");
    }

    log.info("Finished reading {} data sets in {}", i, watch);

    return ImmutableList.of();
  }

  private org.icgc.dcc.repository.ega.util.EGAClient createEGAClient() {
    val userName = System.getProperty("ega.username");
    val password = System.getProperty("ega.password");
    val client = new EGAClient(userName, password);

    client.login();
    return client;
  }

  @Value
  public class EGAMetadata {

    String datasetId;
    List<String> projectCodes;
    List<ObjectNode> files;

    EGAMetadataArchive metadata;

  }

}

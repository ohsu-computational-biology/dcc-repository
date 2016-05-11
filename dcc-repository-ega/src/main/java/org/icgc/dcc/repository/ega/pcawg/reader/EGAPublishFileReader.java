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
package org.icgc.dcc.repository.ega.pcawg.reader;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.icgc.dcc.common.core.util.Splitters.TAB;
import static org.icgc.dcc.repository.ega.pcawg.model.EGAPublishedFile.publishedFile;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.icgc.dcc.repository.ega.pcawg.model.EGAPublishedFile;

import com.google.common.io.Resources;

import lombok.SneakyThrows;
import lombok.val;

public class EGAPublishFileReader extends EGAFileReader<List<EGAPublishedFile>> {

  /**
   * Constants.
   */
  private static final Pattern PUBLISH_FILE_PATTERN = Pattern.compile(""
      // Template:
      // [projectId]/analysis_[type].[study]_[workflow]/[datasetId].files.tsv
      // Example :
      // LICA-FR/analysis_alignment.PCAWG_WGS_BWA/EGAD00001002016.files.tsv
      + "([^/]+)" // [projectId]
      + "/analysis_"
      + "([^.]+)" // [type]
      + "\\."
      + "([^_]+)" // [study]
      + "_"
      + "([^/]+)" // [workflow]
      + "/"
      + "([^.]+)" // [datasetId]
      + ".files.tsv");

  public EGAPublishFileReader(File repoDir) {
    super(repoDir, PUBLISH_FILE_PATTERN);
  }

  @Override
  @SneakyThrows
  protected List<EGAPublishedFile> createFile(Path path, Matcher matcher) {
    val lines = Resources.readLines(path.toUri().toURL(), UTF_8);
    return lines.stream()
        .skip(1)
        .map(TAB::splitToList)
        .map(values -> publishedFile()
            .datasetId(values.get(0))
            .analysisId(values.get(1))
            .fileId(values.get(2))
            .fileName(values.get(3))
            .build())
        .collect(toList());
  }

}

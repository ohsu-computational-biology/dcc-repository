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
package org.icgc.dcc.repository.exacloud.core;

import com.google.common.io.Resources;

import org.icgc.dcc.repository.core.model.RepositoryFile;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.icgc.dcc.common.core.util.Splitters.TAB;
import static org.icgc.dcc.common.core.util.URLs.getUrl;

@Slf4j
public class ExacloudArchiveRepositoryFileProcessor {

  /**
   * Constants.
   */

  public List<RepositoryFile> process(@NonNull String archiveUrl, String projectCode) {
    log.info("Processing archive url '{}'...", archiveUrl);

    val url = getUrl(archiveUrl);

    try {
      val lines = Resources.readLines(url, UTF_8);
      val files = lines.stream()
              .skip(1)
              .map(TAB::splitToList)
              .map(values -> createFile()
                      .setId(values.get(3))
                      .setDonors(
                              Arrays.asList(new RepositoryFile.Donor()
                                  .setDonorId(values.get(1))
                                  .setSampleId(Arrays.asList(values.get(2)))
                                  .setProjectCode(projectCode)
                                  .setSubmittedDonorId(values.get(1))
                                  .setSubmittedSampleId(Arrays.asList(values.get(2)))
                              )
                      )
                      .setFileCopies(Arrays.asList(new RepositoryFile.FileCopy()
                                    .setFileName(values.get(5))
                                    .setRepoFileId(values.get(3))
                                    .setFileFormat(values.get(4))
                                    .setRepoCode("exacloud")
                              )
                      )
                  )
              .collect(toList());
      return files;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;

//    0 = {String@2032} "research-only"
//    1 = {String@2033} "AML-1011"
//    2 = {String@2034} "AML-13-00098"
//    3 = {String@2035} "5e939aa5-26bc-4be5-b017-e63e878cb4c2"
//    4 = {String@2036} "application/octet-stream"
//    5 = {String@2037} "/mnt/lustre1/BeatAML/rnaseq/processed/BeatAML1/alignments/subjunc_alignments/FlowCell1/Sample_13-00098.bai"
//    6 = {String@2038} ""
//    7 = {String@2039} ""
//
  }

  private RepositoryFile createFile() {
    return new RepositoryFile();
  }


}

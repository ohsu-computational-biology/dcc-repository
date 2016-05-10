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
package org.icgc.dcc.repository.ega.reader;

import static com.google.common.collect.Sets.newTreeSet;
import static org.icgc.dcc.repository.ega.util.EGAProjects.getDatasetProjectCodes;

import java.util.stream.Stream;

import org.icgc.dcc.repository.ega.model.EGAMetadata;
import org.icgc.dcc.repository.ega.model.EGAMetadataArchive;
import org.icgc.dcc.repository.ega.util.EGAClient;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class EGAMetadataReader {

  /**
   * Dependencies.
   */
  @NonNull
  private final EGAClient client;
  private final EGAMetadataArchiveReader archiveReader = new EGAMetadataArchiveReader();

  public Stream<EGAMetadata> readMetadata() {
    val datasetIds = client.getDatasetIds();
    val effectiveDatasetIds = newTreeSet(datasetIds);
    if (effectiveDatasetIds.size() != datasetIds.size()) {
      log.warn("Data sets include duplicates: {}", datasetIds);
    }

    return effectiveDatasetIds.stream().map(this::readDataset);
  }

  public EGAMetadata readDataset(String datasetId) {
    val metadata = readArchive(datasetId);
    val files = client.getFiles(datasetId);
    val projectCodes = getDatasetProjectCodes(datasetId);

    return new EGAMetadata(datasetId, projectCodes, files, metadata);
  }

  private EGAMetadataArchive readArchive(String datasetId) {
    return archiveReader.read(datasetId);
  }

}

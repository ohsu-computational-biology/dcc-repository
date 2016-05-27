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
package org.icgc.dcc.repository.pdc.core;

import static java.util.stream.Collectors.toList;

import java.util.List;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.Repository;
import org.icgc.dcc.repository.core.model.RepositoryFile;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PDCFileProcessor extends RepositoryFileProcessor {

  /**
   * Metadata.
   */
  @NonNull
  private final Repository pdcRepository;

  public PDCFileProcessor(RepositoryFileContext context, @NonNull Repository pdcRepository) {
    super(context);
    this.pdcRepository = pdcRepository;
  }

  public Iterable<RepositoryFile> processFiles(List<S3ObjectSummary> objectSummaries) {
    return objectSummaries.stream().map(this::createFile).filter(file -> file != null).collect(toList());
  }

  private RepositoryFile createFile(S3ObjectSummary objectSummary) {
    val objectId = resolveObjectId(objectSummary);

    val objectFile = new RepositoryFile()
        .setId(context.ensureFileId(objectId))
        .setObjectId(objectId);

    objectFile.addFileCopy()
        .setFileSize(objectSummary.getSize())
        .setLastModified(objectSummary.getLastModified().getTime() / 1000L) // Seconds
        .setRepoFileId(objectId)
        .setRepoType(pdcRepository.getType().getId())
        .setRepoOrg(pdcRepository.getSource().getId())
        .setRepoName(pdcRepository.getName())
        .setRepoCode(pdcRepository.getCode())
        .setRepoCountry(pdcRepository.getCountry())
        .setRepoBaseUrl(pdcRepository.getBaseUrl())
        .setRepoDataPath(objectSummary.getBucketName() + pdcRepository.getType().getDataPath() + objectId);

    return objectFile;
  }

  private static String resolveObjectId(S3ObjectSummary objectSummary) {
    val key = objectSummary.getKey();

    // TODO: Remove. Only for development
    if (key.equals("testfile")) {
      log.warn("*** TEMP: mapping!!!!");
      return "9320a69f-296d-5967-a816-3f53d986f59a";
    }

    return null;
  }

}

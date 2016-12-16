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

import com.google.common.collect.ImmutableList;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.Repository;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.exacloud.reader.ExacloudArchiveListReader;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static org.icgc.dcc.common.core.util.Formats.formatCount;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.repository.core.model.Repositories.getExacloudRepository;
import static org.icgc.dcc.repository.core.model.RepositoryProjects.getProjectByProjectCode;

@Slf4j
public class ExacloudFileProcessor extends RepositoryFileProcessor {

  /**
   * Metadata.
   */
  @NonNull
  private final Repository exacloudRepository = getExacloudRepository();

  public ExacloudFileProcessor(RepositoryFileContext context) {
    super(context);
  }

  public Iterable<RepositoryFile> processRepositoryFiles() {
    log.info("Creating repository files...");
    val repositoryFiles = createRepositoryFiles();

    log.info("Filtering repository files...");
    val filteredRepositoryFiles = filterRepositoryFiles(repositoryFiles);

    log.info("Assigning study...");
    assignStudy(filteredRepositoryFiles);

    return filteredRepositoryFiles;
  }

  private Iterable<RepositoryFile> filterRepositoryFiles(Iterable<RepositoryFile> clinicalFiles) {
    return stream(clinicalFiles).filter(hasDonorId()).collect(toImmutableList());
  }

  private Iterable<RepositoryFile> createRepositoryFiles() {
    log.info("Reading archive list entries...");
    val entries = ExacloudArchiveListReader.readEntries();
    log.info("Read {} archive list entries", formatCount(entries));

    val repositoryFiles = ImmutableList.<RepositoryFile> builder();
    for (val entry : entries) {
      val project = getProjectByProjectCode("BAML-US");
      val archiveClinicalFiles = processArchive(project.get().getProjectCode(), entry.getArchiveUrl());
      repositoryFiles.addAll(archiveClinicalFiles);
    }

    return repositoryFiles.build();
  }

  private Iterable<RepositoryFile> processArchive(String projectCode, String archiveUrl) {
    val processor = new ExacloudArchiveRepositoryFileProcessor(context);
    val archiveRepositoryFiles = processor.process(archiveUrl, projectCode);
    log.info("Processing {} archive repository files", formatCount(archiveRepositoryFiles));

    val repositoryFiles = ImmutableList.<RepositoryFile> builder();
    for (val archiveRepositoryFile : archiveRepositoryFiles) {
      if (archiveRepositoryFile != null) {
        repositoryFiles.add(archiveRepositoryFile);
      }
    }

    return repositoryFiles.build();
  }




}

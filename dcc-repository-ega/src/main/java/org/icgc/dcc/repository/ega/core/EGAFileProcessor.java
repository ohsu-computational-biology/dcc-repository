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
package org.icgc.dcc.repository.ega.core;

import java.util.stream.Stream;

import org.elasticsearch.common.collect.Lists;
import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryServers.RepositoryServer;
import org.icgc.dcc.repository.ega.model.EGAMetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EGAFileProcessor extends RepositoryFileProcessor {

  /**
   * Metadata.
   */
  @NonNull
  private final RepositoryServer egaServer;

  public EGAFileProcessor(RepositoryFileContext context, @NonNull RepositoryServer egaServer) {
    super(context);
    this.egaServer = egaServer;
  }

  public Stream<RepositoryFile> process(Stream<EGAMetadata> metadata) {
    return metadata.flatMap(this::createFiles);
  }

  private Stream<RepositoryFile> createFiles(EGAMetadata metadata) {
    val files = Lists.<RepositoryFile> newArrayList();
    for (val file : metadata.getFiles()) {
      files.add(createFile(metadata, file));
    }

    return files.stream();
  }

  private RepositoryFile createFile(EGAMetadata metadata, ObjectNode f) {
    val file = new RepositoryFile();

    file.addFileCopy()
        .setRepoFileId(resolveRepoFileId(f));

    log.info("File: {}", file);

    return file;
  }

  private static String resolveRepoFileId(ObjectNode file) {
    return file.get("fileID").textValue();
  }

}

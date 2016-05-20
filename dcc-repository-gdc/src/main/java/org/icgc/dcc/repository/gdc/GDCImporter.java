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
package org.icgc.dcc.repository.gdc;

import static java.util.stream.Collectors.toList;
import static org.icgc.dcc.repository.core.model.RepositoryServers.getGDCServer;
import static org.icgc.dcc.repository.core.model.RepositorySource.GDC;

import java.util.List;
import java.util.stream.Stream;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.util.GenericRepositorySourceFileImporter;
import org.icgc.dcc.repository.gdc.core.GDCFileProcessor;
import org.icgc.dcc.repository.gdc.util.GDCClient;

import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 */
@Slf4j
public class GDCImporter extends GenericRepositorySourceFileImporter {

  public GDCImporter(RepositoryFileContext context) {
    super(GDC, context, log);
  }

  @Override
  @SneakyThrows
  protected Iterable<RepositoryFile> readFiles() {
    val client = new GDCClient();
    val files = readFiles(client);
    val results = processFiles(files.stream());

    return results.collect(toList());
  }

  private List<ObjectNode> readFiles(GDCClient client) {
    return client.getFiles();
  }

  private Stream<RepositoryFile> processFiles(Stream<ObjectNode> files) {
    return new GDCFileProcessor(context, getGDCServer()).process(files);
  }

}

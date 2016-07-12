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
package org.icgc.dcc.repository.index.core;

import static org.icgc.dcc.repository.core.RepositoryFileContextBuilder.getLocalMongoClientUri;

import java.net.URI;

import org.icgc.dcc.common.core.util.URIs;
import org.junit.Ignore;
import org.junit.Test;

import lombok.Cleanup;
import lombok.val;

@Ignore("For development only")
public class RepositoryFileIndexerTest {

  @Test
  public void testIndexFiles() throws Exception {
    val mongoUri = getLocalMongoClientUri("dcc-repository");
    val esUri = URIs.getUri("es://localhost:9300");
    val archiveUri = getHdfsArchiveUri();
    val indexAlias = "test";

    @Cleanup
    val indexer = new RepositoryFileIndexer(mongoUri, esUri, archiveUri, indexAlias);
    indexer.indexFiles();
  }

  static URI getHdfsArchiveUri() {
    return URIs.getUri("hdfs://hdfs@" + System.getProperty("hostname") + "/tmp/repository.tar.gz");
  }

  static URI getLocalArchiveUri() {
    return URIs.getUri("file:///tmp/repository.tar.gz");
  }

}

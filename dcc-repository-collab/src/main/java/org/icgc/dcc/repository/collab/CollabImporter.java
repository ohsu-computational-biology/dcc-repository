/*
 * Copyright (c) 2015 The Ontario Institute for Cancer Research. All rights reserved.                             
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
package org.icgc.dcc.repository.collab;

import static org.icgc.dcc.repository.core.model.RepositorySource.COLLAB;

import java.util.Collection;

import org.icgc.dcc.repository.cloud.CloudImporter;
import org.icgc.dcc.repository.cloud.core.CloudFileProcessor;
import org.icgc.dcc.repository.cloud.s3.CloudS3BucketReader;
import org.icgc.dcc.repository.cloud.transfer.CloudTransferJobReader;
import org.icgc.dcc.repository.collab.s3.AWSClientFactory;
import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.model.RepositoryServers;

import com.google.common.collect.ImmutableList;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CollabImporter extends CloudImporter {

  /**
   * Constants.
   */
  private static final String BUCKET_NAME = "oicr.icgc";
  private static final String BUCKET_KEY_PREFIX = "data";
  private static final String GIT_REPO_URL = "https://github.com/ICGC-TCGA-PanCancer/ceph_transfer_ops.git";
  private static final Collection<String> GIT_REPO_PATHS = ImmutableList.of(
      "ceph-transfer-jobs/completed-jobs",
      "ceph-transfer-jobs-prod1/completed-jobs",
      "ceph-transfer-jobs-prod2/completed-jobs");

  public CollabImporter(@NonNull RepositoryFileContext context) {
    super(COLLAB, context, log);
  }

  @Override
  protected CloudTransferJobReader createJobReader() {
    return new CloudTransferJobReader(GIT_REPO_URL, GIT_REPO_PATHS);
  }

  @Override
  protected CloudS3BucketReader createBucketReader() {
    val s3 = AWSClientFactory.createS3Client();
    return new CloudS3BucketReader(BUCKET_NAME, BUCKET_KEY_PREFIX, s3);
  }

  @Override
  protected CloudFileProcessor createFileProcessor() {
    val collabServer = RepositoryServers.getCollabServer();
    return new CloudFileProcessor(context, collabServer);
  }

}

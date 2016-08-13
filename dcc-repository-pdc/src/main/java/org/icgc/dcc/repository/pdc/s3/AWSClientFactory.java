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
package org.icgc.dcc.repository.pdc.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.SignerFactory;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.internal.S3Signer;

import lombok.val;

public class AWSClientFactory {

  /**
   * Constants.
   */
  public static final String PDC_OPEN_S3_ENDPOINT = "https://griffin-objstore.opensciencedatacloud.org/";
  public static final String PDC_CS_PROTECTED_S3_ENDPOINT = "https://bionimbus-objstore-cs.opensciencedatacloud.org";
  public static final String PDC_CS_PROTECTED_AWS_PROFILE = "pdc";

  @Deprecated
  public static final String PDC_PROTECTED_S3_ENDPOINT = "https://bionimbus-objstore.opensciencedatacloud.org";

  public static AmazonS3 createOpenS3Client() {
    return createS3Client(PDC_OPEN_S3_ENDPOINT, new StaticCredentialsProvider(new AnonymousAWSCredentials()));
  }

  public static AmazonS3 createProtectedS3Client() {
    return createS3Client(PDC_CS_PROTECTED_S3_ENDPOINT, new ProfileCredentialsProvider(PDC_CS_PROTECTED_AWS_PROFILE));
  }

  private static AmazonS3 createS3Client(String url, AWSCredentialsProvider credentialsProvider) {
    // Required for current version of Rados Gateway
    SignerFactory.registerSigner("S3Signer", S3Signer.class);
    val clientConfiguration = new ClientConfiguration().withSignerOverride("S3Signer");

    val s3 = new AmazonS3Client(credentialsProvider, clientConfiguration);
    s3.setEndpoint(url);
    s3.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));

    return s3;
  }

}

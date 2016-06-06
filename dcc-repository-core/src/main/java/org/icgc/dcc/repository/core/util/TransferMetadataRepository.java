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
package org.icgc.dcc.repository.core.util;

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.CanceledException;
import org.eclipse.jgit.api.errors.DetachedHeadException;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidConfigurationException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.NoHeadException;
import org.eclipse.jgit.api.errors.RefNotAdvertisedException;
import org.eclipse.jgit.api.errors.RefNotFoundException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.api.errors.WrongRepositoryStateException;

import lombok.NonNull;
import lombok.Value;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Value
public class TransferMetadataRepository {

  /**
   * Configuration.
   */
  @NonNull
  private final String repoUrl;
  @NonNull
  private final File repoDir;
  private final boolean forceClone; // To be safe on problematic repos such as EGA

  public void update() throws GitAPIException, IOException {
    if (forceClone && repoDir.exists()) {
      log.info("forceClone is set to true. Deleting {}...", repoDir);
      delete(repoDir);
    }

    if (repoDir.exists()) {
      gitPull();
    } else {
      gitClone();
    }
  }

  private void gitClone() throws GitAPIException, InvalidRemoteException, TransportException {
    checkState(repoDir.mkdirs(), "Could not create '%s'", repoDir);

    log.info("Cloning '{}' to '{}'...", repoUrl, repoDir);
    Git.cloneRepository().setURI(repoUrl).setDirectory(repoDir).call();
    log.info("Finished cloning.");
  }

  private void gitPull() throws GitAPIException, WrongRepositoryStateException, InvalidConfigurationException,
      DetachedHeadException, InvalidRemoteException, CanceledException, RefNotFoundException, RefNotAdvertisedException,
      NoHeadException, TransportException, IOException {
    log.info("Pulling '{}' in '{}'...", repoUrl, repoDir);
    val result = Git.open(repoDir).pull().call();

    checkState(result.isSuccessful(), "Could not successfully pull repository: %s", result);
    log.info("Finished pulling.");
  }

  private static void delete(File file) throws IOException {
    if (file.isDirectory()) {
      for (val f : file.listFiles())
        delete(f);
    }
    if (!file.delete()) throw new FileNotFoundException("Failed to delete file: " + file);
  }

}

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
package org.icgc.dcc.repository.core.model;

import static com.google.common.base.Preconditions.checkState;
import static lombok.AccessLevel.PRIVATE;

import java.util.Arrays;

import org.icgc.dcc.common.core.model.Identifiable;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

@RequiredArgsConstructor(access = PRIVATE)
public enum RepositoryCollection implements Identifiable {

  FILE("File", null),
  EGA_FILE("EGAFile", RepositorySource.EGA),
  CGHUB_FILE("CGHubFile", RepositorySource.CGHUB),
  GDC_FILE("GDCFile", RepositorySource.GDC),
  PDC_FILE("PDCFile", RepositorySource.PDC),
  TCGA_FILE("TCGAFile", RepositorySource.TCGA),
  PCAWG_FILE("PCAWGFile", RepositorySource.PCAWG),
  AWS_FILE("AWSFile", RepositorySource.AWS),
  COLLAB_FILE("CollabFile", RepositorySource.COLLAB);

  @Getter
  @NonNull
  private final String id;

  @Getter
  private final RepositorySource source;

  public static RepositoryCollection forSource(@NonNull RepositorySource source) {
    for (val value : values()) {
      if (source == value.getSource()) {
        return value;
      }
    }

    checkState(false, "Could not find collection for repository source %s. Possible values are: %s",
        source, Arrays.toString(values()));

    return null;
  }

}

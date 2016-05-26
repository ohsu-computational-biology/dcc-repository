/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS"AS IS" AND ANY                           
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

import static com.google.common.collect.Iterables.tryFind;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.repository.core.model.RepositorySource.AWS;
import static org.icgc.dcc.repository.core.model.RepositorySource.CGHUB;
import static org.icgc.dcc.repository.core.model.RepositorySource.COLLAB;
import static org.icgc.dcc.repository.core.model.RepositorySource.EGA;
import static org.icgc.dcc.repository.core.model.RepositorySource.GDC;
import static org.icgc.dcc.repository.core.model.RepositorySource.PCAWG;
import static org.icgc.dcc.repository.core.model.RepositorySource.PDC;
import static org.icgc.dcc.repository.core.model.RepositorySource.TCGA;
import static org.icgc.dcc.repository.core.model.RepositoryType.EGA_ARCHIVE;
import static org.icgc.dcc.repository.core.model.RepositoryType.GDC_ARCHIVE;
import static org.icgc.dcc.repository.core.model.RepositoryType.GNOS;
import static org.icgc.dcc.repository.core.model.RepositoryType.PDC_S3;
import static org.icgc.dcc.repository.core.model.RepositoryType.S3;
import static org.icgc.dcc.repository.core.model.RepositoryType.WEB_ARCHIVE;

import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

@NoArgsConstructor(access = PRIVATE)
public final class RepositoryServers {

  public static class RepositoryCodes {

    public static final String CGHUB = "cghub";
    public static final String EGA = "ega";
    public static final String GDC = "gdc";
    public static final String PDC = "pdc";
    public static final String TCGA = "tcga";
    public static final String PCAWG_BARCELONA = "pcawg-barcelona";
    public static final String PCAWG_CGHUB = "pcawg-cghub";
    public static final String PCAWG_TOKYO = "pcawg-tokyo";
    public static final String PCAWG_SEOUL = "pcawg-seoul";
    public static final String PCAWG_LONDON = "pcawg-london";
    public static final String PCAWG_HEIDELBERG = "pcawg-heidelberg";
    public static final String PCAWG_CHICAGO_ICGC = "pcawg-chicago-icgc";
    public static final String PCAWG_CHICAGO_TCGA = "pcawg-chicago-tcga";
    public static final String AWS_VIRGINIA = "aws-virginia";
    public static final String COLLABORATORY = "collaboratory";

  }

  // @formatter:off
  public static final List<RepositoryServer> SERVERS = ImmutableList.of(
      server().source(EGA)   .type(EGA_ARCHIVE).name("EGA - Hinxton")          .code(RepositoryCodes.EGA)               .country("UK").baseUrl("http://ega.ebi.ac.uk/ega/").build(),
      server().source(GDC)   .type(GDC_ARCHIVE).name("GDC - Chicago")          .code(RepositoryCodes.GDC)               .country("US").baseUrl("https://gdc-api.nci.nih.gov/").build(),
      server().source(PDC)   .type(PDC_S3)     .name("PDC - Chicago")          .code(RepositoryCodes.PDC)               .country("US").baseUrl("https://bionimbus-objstore.opensciencedatacloud.org/").build(),
      server().source(TCGA)  .type(WEB_ARCHIVE).name("TCGA DCC - Bethesda")    .code(RepositoryCodes.TCGA)              .country("US").baseUrl("https://tcga-data.nci.nih.gov/").build(),
      server().source(CGHUB) .type(GNOS)       .name("CGHub - Santa Cruz")     .code(RepositoryCodes.CGHUB)             .country("US").baseUrl("https://cghub.ucsc.edu/").build(),
      server().source(PCAWG) .type(GNOS)       .name("PCAWG - Barcelona")      .code(RepositoryCodes.PCAWG_BARCELONA)   .country("ES").baseUrl("https://gtrepo-bsc.annailabs.com/").build(),
      server().source(PCAWG) .type(GNOS)       .name("PCAWG - Santa Cruz")     .code(RepositoryCodes.PCAWG_CGHUB)       .country("US").baseUrl("https://cghub.ucsc.edu/").build(),
      server().source(PCAWG) .type(GNOS)       .name("PCAWG - Tokyo")          .code(RepositoryCodes.PCAWG_TOKYO)       .country("JP").baseUrl("https://gtrepo-riken.annailabs.com/").build(),
      server().source(PCAWG) .type(GNOS)       .name("PCAWG - Seoul")          .code(RepositoryCodes.PCAWG_SEOUL)       .country("KR").baseUrl("https://gtrepo-etri.annailabs.com/").build(),
      server().source(PCAWG) .type(GNOS)       .name("PCAWG - London")         .code(RepositoryCodes.PCAWG_LONDON)      .country("UK").baseUrl("https://gtrepo-ebi.annailabs.com/").build(),
      server().source(PCAWG) .type(GNOS)       .name("PCAWG - Heidelberg")     .code(RepositoryCodes.PCAWG_HEIDELBERG)  .country("DE").baseUrl("https://gtrepo-dkfz.annailabs.com/").build(),
      server().source(PCAWG) .type(GNOS)       .name("PCAWG - Chicago (ICGC)") .code(RepositoryCodes.PCAWG_CHICAGO_ICGC).country("US").baseUrl("https://gtrepo-osdc-icgc.annailabs.com/").build(),
      server().source(PCAWG) .type(GNOS)       .name("PCAWG - Chicago (TCGA)") .code(RepositoryCodes.PCAWG_CHICAGO_TCGA).country("US").baseUrl("https://gtrepo-osdc-tcga.annailabs.com/").build(),
      server().source(AWS)   .type(S3)         .name("AWS - Virginia")         .code(RepositoryCodes.AWS_VIRGINIA)      .country("US").baseUrl("https://s3-external-1.amazonaws.com/").build(),
      server().source(COLLAB).type(S3)         .name("Collaboratory - Toronto").code(RepositoryCodes.COLLABORATORY)     .country("CA").baseUrl("https://www.cancercollaboratory.org:9080/").build()
      );
  // @formatter:on

  public static Iterable<RepositoryServer> getServers() {
    return SERVERS;
  }

  public static RepositoryServer getCGHubServer() {
    return findServer(server -> server.getSource() == CGHUB);
  }

  public static RepositoryServer getTCGAServer() {
    return findServer(server -> server.getSource() == TCGA);
  }

  public static RepositoryServer getPCAWGServer(String genosRepo) {
    return findServer(server -> server.getSource() == PCAWG && server.getBaseUrl().equals(genosRepo));
  }

  public static RepositoryServer getEGAServer() {
    return findServer(server -> server.getSource() == EGA);
  }

  public static RepositoryServer getGDCServer() {
    return findServer(server -> server.getSource() == GDC);
  }

  public static RepositoryServer getPDCServer() {
    return findServer(server -> server.getSource() == PDC);
  }

  public static RepositoryServer getAWSServer() {
    return findServer(server -> server.getSource() == AWS);
  }

  public static RepositoryServer getCollabServer() {
    return findServer(server -> server.getSource() == COLLAB);
  }

  private static RepositoryServer findServer(Predicate<RepositoryServer> predicate) {
    return tryFind(getServers(), predicate).orNull();
  }

  @Value
  @Builder
  public static class RepositoryServer {

    RepositoryType type;
    RepositorySource source;
    String name;
    String code;
    String country;
    String baseUrl;

  }

  private static RepositoryServer.RepositoryServerBuilder server() {
    return RepositoryServer.builder();
  }

}

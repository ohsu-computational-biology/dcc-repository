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

import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Singular;
import lombok.Value;

@NoArgsConstructor(access = PRIVATE)
public final class Repositories {

  @NoArgsConstructor(access = PRIVATE)
  public static final class RepositoryCodes {

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
  public static final List<Repository> REPOSITORIES = ImmutableList.of(
      repository().source(EGA)   .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.DACO)                                       .storage(RepositoryStorage.EGA) .type(RepositoryType.EGA_ARCHIVE).name("EGA - Hinxton")          .code(RepositoryCodes.EGA)               .country("UK").timezone("Europe/London")      .baseUrl("http://ega.ebi.ac.uk/ega/").build(),
      repository().source(GDC)   .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GDC) .type(RepositoryType.GDC_ARCHIVE).name("GDC - Chicago")          .code(RepositoryCodes.GDC)               .country("US").timezone("America/Chicago")    .baseUrl("https://gdc-api.nci.nih.gov/").build(),
      repository().source(PDC)   .environment(RepositoryEnvironment.OPEN_STACK).access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.S3)  .type(RepositoryType.PDC_S3)     .name("PDC - Chicago")          .code(RepositoryCodes.PDC)               .country("US").timezone("America/Chicago")    .baseUrl("https://bionimbus-objstore.opensciencedatacloud.org/").build(),
      repository().source(TCGA)  .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.OPEN)                                       .storage(RepositoryStorage.WEB) .type(RepositoryType.WEB_ARCHIVE).name("TCGA DCC - Bethesda")    .code(RepositoryCodes.TCGA)              .country("US").timezone("America/New_York")   .baseUrl("https://tcga-data.nci.nih.gov/").build(),
      repository().source(CGHUB) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("CGHub - Santa Cruz")     .code(RepositoryCodes.CGHUB)             .country("US").timezone("America/Los_Angeles").baseUrl("https://cghub.ucsc.edu/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Barcelona")      .code(RepositoryCodes.PCAWG_BARCELONA)   .country("ES").timezone("Europe/Madrid")      .baseUrl("https://gtrepo-bsc.annailabs.com/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Santa Cruz")     .code(RepositoryCodes.PCAWG_CGHUB)       .country("US").timezone("America/Los_Angeles").baseUrl("https://cghub.ucsc.edu/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Tokyo")          .code(RepositoryCodes.PCAWG_TOKYO)       .country("JP").timezone("Asia/Tokyo")         .baseUrl("https://gtrepo-riken.annailabs.com/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Seoul")          .code(RepositoryCodes.PCAWG_SEOUL)       .country("KR").timezone("Asia/Seoul")         .baseUrl("https://gtrepo-etri.annailabs.com/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - London")         .code(RepositoryCodes.PCAWG_LONDON)      .country("UK").timezone("Europe/London")      .baseUrl("https://gtrepo-ebi.annailabs.com/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Heidelberg")     .code(RepositoryCodes.PCAWG_HEIDELBERG)  .country("DE").timezone("Europe/Berlin")      .baseUrl("https://gtrepo-dkfz.annailabs.com/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Chicago (ICGC)") .code(RepositoryCodes.PCAWG_CHICAGO_ICGC).country("US").timezone("America/Chicago")    .baseUrl("https://gtrepo-osdc-icgc.annailabs.com/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Chicago (TCGA)") .code(RepositoryCodes.PCAWG_CHICAGO_TCGA).country("US").timezone("America/Chicago")    .baseUrl("https://gtrepo-osdc-tcga.annailabs.com/").build(),
      repository().source(AWS)   .environment(RepositoryEnvironment.AWS)       .access(RepositoryAccess.DACO)                                       .storage(RepositoryStorage.ICGC).type(RepositoryType.S3)         .name("AWS - Virginia")         .code(RepositoryCodes.AWS_VIRGINIA)      .country("US").timezone("America/New_York")   .baseUrl("https://s3-external-1.amazonaws.com/").build(),
      repository().source(COLLAB).environment(RepositoryEnvironment.OPEN_STACK).access(RepositoryAccess.DACO)                                       .storage(RepositoryStorage.ICGC).type(RepositoryType.S3)         .name("Collaboratory - Toronto").code(RepositoryCodes.COLLABORATORY)     .country("CA").timezone("America/Toronto")    .baseUrl("https://www.cancercollaboratory.org:9080/").build()
      );
  // @formatter:on

  public static Iterable<Repository> getRepositories() {
    return REPOSITORIES;
  }

  public static Repository getCGHubRepository() {
    return findRepository(repository -> repository.getSource() == CGHUB);
  }

  public static Repository getTCGARepository() {
    return findRepository(repository -> repository.getSource() == TCGA);
  }

  public static Repository getPCAWGRepository(String genosRepo) {
    return findRepository(repository -> repository.getSource() == PCAWG && repository.getBaseUrl().equals(genosRepo));
  }

  public static Repository getEGARepository() {
    return findRepository(repository -> repository.getSource() == EGA);
  }

  public static Repository getGDCRepository() {
    return findRepository(repository -> repository.getSource() == GDC);
  }

  public static Repository getPDCRepository() {
    return findRepository(repository -> repository.getSource() == PDC);
  }

  public static Repository getAWSRepository() {
    return findRepository(repository -> repository.getSource() == AWS);
  }

  public static Repository getCollabRepository() {
    return findRepository(repository -> repository.getSource() == COLLAB);
  }

  private static Repository findRepository(Predicate<Repository> predicate) {
    return tryFind(getRepositories(), predicate).orNull();
  }

  @Value
  @Builder
  public static class Repository {

    RepositoryType type;
    RepositorySource source;
    String name;
    String code;
    String country;
    String timezone;
    String baseUrl;

    RepositoryStorage storage;
    RepositoryEnvironment environment;
    String organization;
    @Singular("access")
    List<RepositoryAccess> access;

    String description;
    String email;

    String dataUrl;
    String accessUrl;
    String metadataUrl;
    String registrationUrl;

  }

  private static Repository.RepositoryBuilder repository() {
    return Repository.builder();
  }

}

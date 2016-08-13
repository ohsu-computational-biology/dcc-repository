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
import static com.google.common.collect.Maps.uniqueIndex;
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
import java.util.Map;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor(access = PRIVATE)
public final class Repositories {

  // @formatter:off
  private static final List<Repository> REPOSITORIES = ImmutableList.of(
      repository().source(EGA)   .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.DACO)                                       .storage(RepositoryStorage.EGA) .type(RepositoryType.EGA_ARCHIVE).name("EGA - Hinxton")          .code("ega")               .country("UK").timezone("Europe/London")      .baseUrl("http://ega.ebi.ac.uk/ega/").build(),
      repository().source(GDC)   .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GDC) .type(RepositoryType.GDC_ARCHIVE).name("GDC - Chicago")          .code("gdc")               .country("US").timezone("America/Chicago")    .baseUrl("https://gdc-api.nci.nih.gov/").build(),
      repository().source(PDC)   .environment(RepositoryEnvironment.OPEN_STACK).access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.S3)  .type(RepositoryType.PDC_S3)     .name("PDC - Chicago")          .code("pdc")               .country("US").timezone("America/Chicago")    .baseUrl("https://bionimbus-objstore-cs.opensciencedatacloud.org/").build(),
      repository().source(TCGA)  .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.OPEN)                                       .storage(RepositoryStorage.WEB) .type(RepositoryType.WEB_ARCHIVE).name("TCGA DCC - Bethesda")    .code("tcga")              .country("US").timezone("America/New_York")   .baseUrl("https://tcga-data.nci.nih.gov/").build(),
      repository().source(CGHUB) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("CGHub - Santa Cruz")     .code("cghub")             .country("US").timezone("America/Los_Angeles").baseUrl("https://cghub.ucsc.edu/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Barcelona")      .code("pcawg-barcelona")   .country("ES").timezone("Europe/Madrid")      .baseUrl("https://gtrepo-bsc.annailabs.com/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Santa Cruz")     .code("pcawg-cghub")       .country("US").timezone("America/Los_Angeles").baseUrl("https://cghub.ucsc.edu/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Tokyo")          .code("pcawg-tokyo")       .country("JP").timezone("Asia/Tokyo")         .baseUrl("https://gtrepo-riken.annailabs.com/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Seoul")          .code("pcawg-seoul")       .country("KR").timezone("Asia/Seoul")         .baseUrl("https://gtrepo-etri.annailabs.com/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - London")         .code("pcawg-london")      .country("UK").timezone("Europe/London")      .baseUrl("https://gtrepo-ebi.annailabs.com/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Heidelberg")     .code("pcawg-heidelberg")  .country("DE").timezone("Europe/Berlin")      .baseUrl("https://gtrepo-dkfz.annailabs.com/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Chicago (ICGC)") .code("pcawg-chicago-icgc").country("US").timezone("America/Chicago")    .baseUrl("https://gtrepo-osdc-icgc.annailabs.com/").build(),
      repository().source(PCAWG) .environment(RepositoryEnvironment.INTERNET)  .access(RepositoryAccess.ERA_COMMONS).access(RepositoryAccess.DB_GAP).storage(RepositoryStorage.GNOS).type(RepositoryType.GNOS)       .name("PCAWG - Chicago (TCGA)") .code("pcawg-chicago-tcga").country("US").timezone("America/Chicago")    .baseUrl("https://gtrepo-osdc-tcga.annailabs.com/").build(),
      repository().source(AWS)   .environment(RepositoryEnvironment.AWS)       .access(RepositoryAccess.DACO)                                       .storage(RepositoryStorage.ICGC).type(RepositoryType.S3)         .name("AWS - Virginia")         .code("aws-virginia")      .country("US").timezone("America/New_York")   .baseUrl("https://s3-external-1.amazonaws.com/").build(),
      repository().source(COLLAB).environment(RepositoryEnvironment.OPEN_STACK).access(RepositoryAccess.DACO)                                       .storage(RepositoryStorage.ICGC).type(RepositoryType.S3)         .name("Collaboratory - Toronto").code("collaboratory")     .country("CA").timezone("America/Toronto")    .baseUrl("https://www.cancercollaboratory.org:9080/").build()
      );
  // @formatter:on

  private static final Map<String, Repository> INDEX = uniqueIndex(REPOSITORIES, r -> r.getCode());

  public static Repository getRepository(@NonNull String repoCode) {
    return INDEX.get(repoCode);
  }

  public static Iterable<Repository> getRepositories() {
    return REPOSITORIES;
  }

  @Getter
  private static final Repository cGHubRepository = findRepository(repository -> repository.getSource() == CGHUB);
  @Getter
  private static final Repository tCGARepository = findRepository(repository -> repository.getSource() == TCGA);
  @Getter
  private static final Repository eGARepository = findRepository(repository -> repository.getSource() == EGA);
  @Getter
  private static final Repository gDCRepository = findRepository(repository -> repository.getSource() == GDC);
  @Getter
  private static final Repository pDCRepository = findRepository(repository -> repository.getSource() == PDC);
  @Getter
  private static final Repository aWSRepository = findRepository(repository -> repository.getSource() == AWS);
  @Getter
  private static final Repository collabRepository = findRepository(repository -> repository.getSource() == COLLAB);

  private static Repository findRepository(Predicate<Repository> predicate) {
    return tryFind(getRepositories(), predicate).orNull();
  }

  private static Repository.RepositoryBuilder repository() {
    return Repository.builder();
  }

  public static Repository getPCAWGRepository(@NonNull String gnosUrl) {
    return findRepository(repository -> repository.getSource() == PCAWG && repository.getBaseUrl().equals(gnosUrl));
  }

}

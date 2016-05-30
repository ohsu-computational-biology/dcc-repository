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
package org.icgc.dcc.repository.core;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.icgc.dcc.common.core.tcga.TCGAIdentifiers.isUUID;
import static org.icgc.dcc.common.core.util.Formats.formatCount;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.repository.core.model.RepositoryProjects.getTCGAProjects;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.icgc.dcc.common.core.util.UUID5;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.Donor;
import org.icgc.dcc.repository.core.model.RepositoryFile.Study;
import org.icgc.dcc.repository.core.util.MetadataClient;
import org.icgc.dcc.repository.core.util.MetadataClient.Entity;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public abstract class RepositoryFileProcessor {

  /**
   * Dependencies.
   */
  @NonNull
  protected final RepositoryFileContext context;
  private final MetadataClient metadataClient = new MetadataClient();

  protected void assignStudy(Iterable<RepositoryFile> files) {
    eachFileDonor(files, donor -> {
      boolean pcawg = context.isPCAWGSubmittedDonorId(donor.getProjectCode(), donor.getSubmittedDonorId());
      if (pcawg) {
        donor.setStudy(Study.PCAWG);
      }
    });
  }

  protected void assignIds(Iterable<RepositoryFile> donorFiles) {
    val tcgaProjectCodes = resolveTCGAProjectCodes();

    for (val donorFile : donorFiles) {
      for (val donor : donorFile.getDonors()) {
        val projectCode = donor.getProjectCode();

        // Special case for TCGA who submits barcodes to DCC but UUIDs to PCAWG
        val tcga = tcgaProjectCodes.contains(donor.getProjectCode());
        val submittedDonorId =
            tcga ? donor.getOtherIdentifiers().getTcgaParticipantBarcode() : donor.getSubmittedDonorId();
        val submittedSpecimenId =
            tcga ? donor.getOtherIdentifiers().getTcgaSampleBarcode() : donor.getSubmittedSpecimenId();
        val submittedSampleId =
            tcga ? donor.getOtherIdentifiers().getTcgaAliquotBarcode() : donor.getSubmittedSampleId();

        // Get IDs or create if they don't exist. This is different than the other repos.
        donor
            .setDonorId(
                submittedDonorId == null ? null : context.ensureDonorId(submittedDonorId, projectCode))
            .setSpecimenId(
                submittedSpecimenId == null ? null : context.ensureSpecimenId(submittedSpecimenId, projectCode))
            .setSampleId(
                submittedSampleId == null ? null : context.ensureSampleId(submittedSampleId, projectCode));
      }
    }
  }

  protected void translateTCGAUUIDs(Iterable<RepositoryFile> donorFiles) {
    log.info("Collecting TCGA barcodes...");
    val uuids = resolveTCGAUUIDs(donorFiles);

    log.info("Translating {} TCGA barcodes to TCGA UUIDs...", formatCount(uuids));
    val barcodes = context.getTCGABarcodes(uuids);
    eachFileDonor(donorFiles, donor -> donor.getOtherIdentifiers()
        .setTcgaParticipantBarcode(barcodes.get(donor.getSubmittedDonorId()))
        .setTcgaSampleBarcode(barcodes.get(donor.getSubmittedSpecimenId()))
        .setTcgaAliquotBarcode(barcodes.get(donor.getSubmittedSampleId())));
  }

  protected Optional<Entity> findEntity(@NonNull String objectId) {
    return metadataClient.findEntity(objectId);
  }

  protected static Set<String> resolveTCGAUUIDs(Iterable<RepositoryFile> donorFiles) {
    val tcgaProjectCodes = resolveTCGAProjectCodes();
    val uuids = Sets.<String> newHashSet();
    for (val donorFile : donorFiles) {
      for (val donor : donorFile.getDonors()) {
        val donorId = donor.getSubmittedDonorId();
        val specimenId = donor.getSubmittedSpecimenId();
        val sampleId = donor.getSubmittedSampleId();

        val tcga = tcgaProjectCodes.contains(donor.getProjectCode());
        if (!tcga) {
          continue;
        }

        if (isUUID(donorId)) {
          uuids.add(donorId);
        }
        if (isUUID(specimenId)) {
          uuids.add(specimenId);
        }
        if (isUUID(sampleId)) {
          uuids.add(sampleId);
        }
      }
    }

    return uuids;
  }

  //
  // Utilities
  //

  protected static List<String> studies(String... values) {
    return ImmutableList.copyOf(values);
  }

  protected static Stream<Donor> streamFileDonors(@NonNull Iterable<RepositoryFile> files) {
    return stream(files).flatMap(file -> file.getDonors().stream());
  }

  protected static void eachFileDonor(@NonNull Iterable<RepositoryFile> files, @NonNull Consumer<Donor> consumer) {
    streamFileDonors(files).forEach(consumer);
  }

  protected static Predicate<? super RepositoryFile> hasDonorId() {
    return (RepositoryFile file) -> file.getDonors().stream().anyMatch(donor -> donor.hasDonorId());
  }

  protected static Predicate<? super RepositoryFile> hasDataType() {
    return donorFile -> !isNullOrEmpty(donorFile.getDataCategorization().getDataType());
  }

  protected static String resolveObjectId(String... parts) {
    return UUID5.fromUTF8(UUID5.getNamespace(), Joiner.on('/').join(parts)).toString();
  }

  protected static Set<String> resolveTCGAProjectCodes() {
    return stream(getTCGAProjects()).map(project -> project.getProjectCode()).collect(toImmutableSet());
  }

}

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
package org.icgc.dcc.repository.pcawg.core;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.primitives.Longs.max;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.Collections.singleton;
import static org.icgc.dcc.common.core.tcga.TCGAIdentifiers.isUUID;
import static org.icgc.dcc.common.core.util.FormatUtils.formatCount;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.repository.core.model.RepositoryProjects.getProjectCodeProject;
import static org.icgc.dcc.repository.core.model.RepositoryProjects.getTCGAProjects;
import static org.icgc.dcc.repository.core.model.RepositoryServers.getPCAWGServer;
import static org.icgc.dcc.repository.pcawg.core.PCAWGFileInfoResolver.resolveDataCategorization;
import static org.icgc.dcc.repository.pcawg.core.PCAWGFileInfoResolver.resolveFileFormat;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.PCAWG_LIBRARY_STRATEGY_NAMES;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.PCAWG_SPECIMEN_CLASSES;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.PCAWG_WORKFLOW_TYPES;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getBamFileMd5sum;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getBamFileName;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getBamFileSize;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getDccProjectCode;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getFileMd5sum;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getFileName;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getFileSize;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getFiles;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getGnosId;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getGnosLastModified;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getGnosRepo;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getSpecimenType;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getSubmitterDonorId;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getSubmitterSampleId;
import static org.icgc.dcc.repository.pcawg.util.PCAWGArchives.getSubmitterSpecimenId;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.Donor;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileCopy;
import org.icgc.dcc.repository.core.model.RepositoryFile.OtherIdentifiers;
import org.icgc.dcc.repository.core.model.RepositoryServers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PCAWGDonorProcessor extends RepositoryFileProcessor {

  public PCAWGDonorProcessor(RepositoryFileContext context) {
    super(context);
  }

  public Iterable<RepositoryFile> processDonors(@NonNull Iterable<ObjectNode> donors) {
    // Key steps, order matters
    val donorFiles = createDonorFiles(donors);
    val filteredFiles = filterFiles(donorFiles);
    translateUUIDs(filteredFiles);
    assignIds(filteredFiles);

    return filteredFiles;
  }

  private Iterable<RepositoryFile> createDonorFiles(Iterable<ObjectNode> donors) {
    return stream(donors).flatMap(donor -> stream(processDonor(donor))).collect(toImmutableList());
  }

  private Iterable<RepositoryFile> filterFiles(Iterable<RepositoryFile> donorFiles) {
    return filter(donorFiles, donorFile -> !isNullOrEmpty(donorFile.getDataCategorization().getDataType()));
  }

  private void translateUUIDs(Iterable<RepositoryFile> donorFiles) {
    log.info("Collecting TCGA barcodes...");
    val uuids = resolveUUIDs(donorFiles);

    log.info("Translating {} TCGA barcodes to TCGA UUIDs...", formatCount(uuids));
    val barcodes = context.getTCGABarcodes(uuids);
    for (val donorFile : donorFiles) {
      for (val donor : donorFile.getDonors()) {
        donor.getOtherIdentifiers()
            .setTcgaParticipantBarcode(barcodes.get(donor.getSubmittedDonorId()))
            .setTcgaSampleBarcode(barcodes.get(donor.getSubmittedSpecimenId()))
            .setTcgaAliquotBarcode(barcodes.get(donor.getSubmittedSampleId()));
      }
    }
  }

  private void assignIds(Iterable<RepositoryFile> donorFiles) {
    val tcgaProjectCodes = resolveTCGAProjectCodes();

    log.info("Assigning ICGC ids...");
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

  private Iterable<RepositoryFile> processDonor(@NonNull ObjectNode donor) {
    val projectCode = getDccProjectCode(donor);
    val submittedDonorId = getSubmitterDonorId(donor);
    val donorFiles = ImmutableList.<RepositoryFile> builder();

    for (val libraryStrategyName : PCAWG_LIBRARY_STRATEGY_NAMES) {
      for (val specimenClass : PCAWG_SPECIMEN_CLASSES) {
        val specimens = donor.path(libraryStrategyName).path(specimenClass);

        for (val specimen : specimens.isArray() ? specimens : singleton(specimens)) {
          for (val workflowType : PCAWG_WORKFLOW_TYPES) {
            val workflow = specimen.path(workflowType);
            val analysisType = resolveAnalysisType(libraryStrategyName, specimenClass, workflowType);
            for (val workflowFile : getFiles(workflow)) {
              val donorFile = createDonorFile(projectCode, submittedDonorId, analysisType, workflow, workflowFile);
              donorFiles.add(donorFile);
            }
          }
        }
      }
    }

    return donorFiles.build();
  }

  private RepositoryFile createDonorFile(String projectCode, String submittedDonorId, String analysisType,
      JsonNode workflow, JsonNode workflowFile) {
    val project = getProjectCodeProject(projectCode).orNull();
    checkState(project != null, "No project found for project code '%s'", projectCode);

    val gnosId = getGnosId(workflow);
    val specimenType = getSpecimenType(workflow);
    val submitterSpecimenId = getSubmitterSpecimenId(workflow);
    val submitterSampleId = getSubmitterSampleId(workflow);

    val fileName = resolveFileName(workflowFile);
    val fileSize = resolveFileSize(workflowFile);
    val fileFormat = resolveFileFormat(analysisType, fileName);
    val dataCategorization = resolveDataCategorization(analysisType, fileName);
    val id = resolveId(gnosId, fileName);

    val pcawgServers = resolvePCAWGServers(workflow);

    val donorFile = new RepositoryFile()
        .setId(id)
        .setFileId(context.ensureFileId(id))
        .setStudy(ImmutableList.of(PCAWG_STUDY_VALUE))
        .setAccess("controlled")
        .setDataCategorization(dataCategorization);

    donorFile.getDataBundle()
        .setDataBundleId(gnosId);

    for (val pcawgServer : pcawgServers) {
      donorFile.getFileCopies().add(new FileCopy()
          .setRepoType(pcawgServer.getType().getId())
          .setRepoOrg(pcawgServer.getSource().getId())
          .setRepoMetadataPath(pcawgServer.getType().getMetadataPath())
          .setRepoDataPath(pcawgServer.getType().getDataPath())
          .setFileName(fileName)
          .setFileFormat(fileFormat)
          .setFileMd5sum(resolveMd5sum(workflowFile))
          .setFileSize(fileSize)
          .setLastModified(resolveLastModified(workflow)));
    }

    donorFile.getDonors().add(new Donor()
        .setPrimarySite(context.getPrimarySite(projectCode))
        .setProgram(project.getProgram())
        .setProjectCode(projectCode)
        .setStudy(PCAWG_STUDY_VALUE)
        .setDonorId(null) // Set downstream
        .setSpecimenId(null) // Set downstream
        .setSpecimenType(specimenType)
        .setSampleId(null) // Set downstream
        .setSubmittedDonorId(submittedDonorId)
        .setSubmittedSpecimenId(submitterSpecimenId)
        .setSubmittedSampleId(submitterSampleId)
        .setOtherIdentifiers(new OtherIdentifiers()
            .setTcgaParticipantBarcode(null) // Set downstream
            .setTcgaSampleBarcode(null) // Set downstream
            .setTcgaAliquotBarcode(null))); // Set downstream

    return donorFile;
  }

  private static List<RepositoryServers.RepositoryServer> resolvePCAWGServers(JsonNode workflow) {
    return stream(getGnosRepo(workflow))
        .map(genosRepo -> getPCAWGServer(genosRepo.asText()))
        .collect(toImmutableList());
  }

  private static String resolveAnalysisType(String libraryStrategyName, String specimenClass, String workflowType) {
    return libraryStrategyName + "." + specimenClass + "." + workflowType;
  }

  private static Set<String> resolveUUIDs(Iterable<RepositoryFile> donorFiles) {
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

  private static Set<String> resolveTCGAProjectCodes() {
    return stream(getTCGAProjects()).map(project -> project.getProjectCode()).collect(toImmutableSet());
  }

  private static String resolveFileName(JsonNode workflowFile) {
    return firstNonNull(getBamFileName(workflowFile), getFileName(workflowFile));
  }

  private static String resolveMd5sum(JsonNode workflowFile) {
    return firstNonNull(getBamFileMd5sum(workflowFile), getFileMd5sum(workflowFile));
  }

  private static long resolveFileSize(JsonNode workflowFile) {
    // First non-zero
    return max(getFileSize(workflowFile), getBamFileSize(workflowFile));
  }

  private static long resolveLastModified(JsonNode workflow) {
    val text = getGnosLastModified(workflow);
    val dateTime = ISO_OFFSET_DATE_TIME.parse(text, Instant::from);

    return dateTime.getEpochSecond();
  }

}

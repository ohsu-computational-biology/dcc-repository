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
package org.icgc.dcc.repository.gdc.core;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.Collections.singleton;
import static org.icgc.dcc.common.core.util.Formats.formatCount;
import static org.icgc.dcc.repository.core.model.RepositoryProjects.getProjectCodeProject;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getAccess;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getAliquotId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getAliquotSubmitterId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getAnalysisId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getAnalysisWorkflowType;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getAnalyteAliquots;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getCaseId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getCaseProjectId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getCaseProjectPrimarySite;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getCaseSampleId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getCaseSampleType;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getCaseSamples;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getCases;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getDataCategory;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getDataFormat;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getDataType;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getExperimentalStrategy;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getFileId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getFileName;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getFileSize;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getIndexDataFormat;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getIndexFileId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getIndexFileName;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getIndexFileSize;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getIndexFiles;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getIndexMd5sum;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getMd5sum;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getPortionAnalytes;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getSamplePortions;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getSampleSubmitterId;
import static org.icgc.dcc.repository.gdc.util.GDCFiles.getUpdatedDatetime;
import static org.icgc.dcc.repository.gdc.util.GDCProjects.getProjectCode;

import java.time.Instant;
import java.util.stream.Stream;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.Repositories.Repository;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.OtherIdentifiers;
import org.icgc.dcc.repository.core.model.RepositoryFile.ReferenceGenome;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * Maps GDC files to ICGC repository file model.
 * 
 * @see https://wiki.oicr.on.ca/pages/viewpage.action?pageId=66946440
 */
@Slf4j
public class GDCFileProcessor extends RepositoryFileProcessor {

  /**
   * Constants.
   */
  private static final ReferenceGenome GDC_REFERENCE_GENOME = new ReferenceGenome()
      .setGenomeBuild("GRCh38.p0")
      .setReferenceName("GRCh38.d1.vd1");

  /**
   * Metadata.
   */
  @NonNull
  private final Repository gdcRepository;

  /**
   * State.
   */
  private int fileCount = 0;

  public GDCFileProcessor(RepositoryFileContext context, @NonNull Repository gdcRepository) {
    super(context);
    this.gdcRepository = gdcRepository;
  }

  public Stream<RepositoryFile> process(Stream<ObjectNode> files) {
    return files.map(this::createFile).filter(file -> file != null);
  }

  private RepositoryFile createFile(ObjectNode file) {
    val dataType = resolveDataType(file);
    if (dataType == null) {
      // Ignored
      return null;
    }

    val fileId = getFileId(file);
    val objectId = resolveObjectId(fileId);

    val gdcFile = new RepositoryFile()
        .setId(context.ensureFileId(objectId))
        .setStudy(null) // N/A
        .setObjectId(null); // N/A

    gdcFile.setAccess(getAccess(file));

    gdcFile.getAnalysisMethod()
        .setSoftware(null) // N/A
        .setAnalysisType(resolveAnalysisType(file));

    gdcFile.getDataCategorization()
        .setExperimentalStrategy(getExperimentalStrategy(file))
        .setDataType(dataType);

    val dataBundleId = resolveDataBundleId(file);
    gdcFile.getDataBundle()
        .setDataBundleId(dataBundleId);

    gdcFile.setReferenceGenome(GDC_REFERENCE_GENOME);

    val fileCopy = gdcFile.addFileCopy()
        .setRepoDataBundleId(dataBundleId)
        .setRepoFileId(fileId)
        .setRepoDataSetId(null) // N/A
        .setFileFormat(getDataFormat(file))
        .setFileSize(getFileSize(file))
        .setFileName(getFileName(file))
        .setFileMd5sum(getMd5sum(file))
        .setLastModified(resolveLastModified(file))
        .setRepoType(gdcRepository.getType().getId())
        .setRepoOrg(gdcRepository.getSource().getId())
        .setRepoName(gdcRepository.getName())
        .setRepoCode(gdcRepository.getCode())
        .setRepoCountry(gdcRepository.getCountry())
        .setRepoBaseUrl(gdcRepository.getBaseUrl())
        .setRepoMetadataPath(gdcRepository.getType().getMetadataPath())
        .setRepoDataPath(gdcRepository.getType().getDataPath());

    for (val indexFile : getIndexFiles(file)) {
      // TODO: Arbitrary selection of one from many
      val indexFileId = getIndexFileId(indexFile);
      val indexObjectId = resolveObjectId(indexFileId);
      fileCopy.getIndexFile()
          .setId(context.ensureFileId(indexObjectId))
          .setObjectId(null) // N/A
          .setRepoFileId(indexFileId)
          .setFileName(getIndexFileName(indexFile))
          .setFileFormat(getIndexDataFormat(indexFile))
          .setFileSize(getIndexFileSize(indexFile))
          .setFileMd5sum(getIndexMd5sum(indexFile));
      break;
    }

    for (val caze : getCases(file)) {
      val projectCode = resolveProjectCode(caze);
      val project = getProjectCodeProject(projectCode).orNull();

      gdcFile.addDonor()
          .setPrimarySite(resolvePrimarySite(caze, projectCode))
          .setProgram(project.getProgram())
          .setProjectCode(projectCode)
          .setStudy(null) // N/A
          .setDonorId(null) // Set downstream
          .setSpecimenId(null) // Set downstream
          .setSpecimenType(resolveSpecimenType(caze))
          .setSampleId(null) // Set downstream
          .setSubmittedDonorId(resolveSubmittedDonorId(caze))
          .setSubmittedSpecimenId(resolveSubmitterSpecimenId(caze))
          .setSubmittedSampleId(resolveSubmitterSampleId(caze))
          .setOtherIdentifiers(new OtherIdentifiers()
              .setTcgaParticipantBarcode(resolveTcgaParticipantBarcode(caze))
              .setTcgaSampleBarcode(resolveTcgaSampleBarcode(caze))
              .setTcgaAliquotBarcode(resolveTcgaAliquotBarcode(caze)));

    }

    // "Downstream"
    assignIds(singleton(gdcFile));

    if (++fileCount % 1000 == 0) {
      log.info("Processed {} files", formatCount(fileCount));
    }

    return gdcFile;
  }

  private String resolvePrimarySite(JsonNode caze, String projectCode) {
    val primarySite = context.getPrimarySite(projectCode);
    if (primarySite != null) {
      return primarySite;
    }

    return getCaseProjectPrimarySite(caze);
  }

  private static String resolveAnalysisType(@NonNull ObjectNode file) {
    return getAnalysisWorkflowType(file);
  }

  private static String resolveDataBundleId(@NonNull ObjectNode file) {
    return getAnalysisId(file);
  }

  /**
   * @see https://wiki.oicr.on.ca/pages/viewpage.action?pageId=66946440#
   * ICGCrepositorymetadataJSONmodelupdatetoaccommodateEGA/GDCintegration-Datacategory/typemapping
   */
  private static String resolveDataType(@NonNull ObjectNode file) {
    // Inputs
    val dataCategory = getDataCategory(file);
    val dataType = getDataType(file);

    // Special cases
    final String ignored = null, tbd = ignored, unexpected = dataCategory + " - " + dataType;

    // Output
    switch (dataCategory) {

    case "Raw Sequencing Data":
      switch (dataType) {
      case "Aligned Reads":
        return dataType;
      case "Aligned Reads Index":
      case "Experiment Metadata":
      case "Run Metadata":
      case "Analysis Metadata":
        return ignored;
      default:
        return unexpected;
      }

    case "Simple Nucleotide Variation":
      switch (dataType) {
      case "Simple Somatic Mutation":
      case "Simple Germline Variation":
        return dataType;
      case "Aggregated Somatic Mutations":
        return "Simple Somatic Mutation";
      default:
        return unexpected;
      }

    case "Copy Number Variation":
      switch (dataType) {
      case "Copy Number Somatic Variation":
        return "Copy Number Somatic Mutation";
      case "Copy Number Germline Variation":
        return dataType;
      default:
        return unexpected;
      }

    case "Transcriptome Profiling":
      switch (dataType) {
      case "Gene Expression Quantifcation":
      case "Exon Expression Quantification":
      case "miRNA Expression Quantification":
        return dataType;
      default:
        return unexpected;
      }

    case "Clinical":
      switch (dataType) {
      case "Clinical Supplement":
        return "Clinical Data";
      case "Pathology Report":
        return dataType;
      default:
        return unexpected;
      }

    case "Biospecimen":
      switch (dataType) {
      case "Biospecimen Supplement":
        return "Biospecimen Data";
      case "Slide Image":
        return dataType;
      default:
        return unexpected;
      }

    case "Structural Rearrangement":
      switch (dataType) {
      case "Structural Somatic Rearrangement":
        return "Structural Somatic Mutation";
      case "Structural Germline Rearrangement":
        return "Structural Germline Variants";
      default:
        return unexpected;
      }

    case "DNA Methylation":
      switch (dataType) {
      case "Raw Intensity":
      case "Probe Intensity":
      case "CpG Beta Value":
        return tbd;
      default:
        return unexpected;
      }

    case "Protein Expression":
      switch (dataType) {
      case "Array Slide Image":
      case "RPPA Slide Image Measurements":
      case "Normalized Protein Expression":
        return tbd;
      default:
        return unexpected;
      }

    default:
      return unexpected;
    }
  }

  private static Long resolveLastModified(@NonNull ObjectNode file) {
    val text = getUpdatedDatetime(file);
    val temporal = ISO_OFFSET_DATE_TIME.parse(text);

    return Instant.from(temporal).getEpochSecond();
  }

  private static String resolveProjectCode(@NonNull JsonNode caze) {
    val projectId = getCaseProjectId(caze);
    return getProjectCode(projectId);
  }

  private static String resolveSubmittedDonorId(JsonNode caze) {
    return getCaseId(caze);
  }

  private static String resolveSubmitterSpecimenId(JsonNode caze) {
    for (val sample : getCaseSamples(caze)) {
      // TODO: Arbitrary selection of one from many
      return getCaseSampleId(sample);
    }

    return null;
  }

  private static String resolveSpecimenType(JsonNode caze) {
    for (val sample : getCaseSamples(caze)) {
      // TODO: Arbitrary selection of one from many
      return getCaseSampleType(sample);
    }

    return null;
  }

  private static String resolveSubmitterSampleId(JsonNode caze) {
    for (val sample : getCaseSamples(caze)) {
      for (val portion : getSamplePortions(sample)) {
        for (val analyte : getPortionAnalytes(portion)) {
          for (val aliquot : getAnalyteAliquots(analyte)) {
            // TODO: Arbitrary selection of one from many
            return getAliquotId(aliquot);
          }
        }
      }
    }

    return null;
  }

  private static String resolveTcgaParticipantBarcode(JsonNode caze) {
    return getAliquotSubmitterId(caze);
  }

  private static String resolveTcgaAliquotBarcode(JsonNode caze) {
    for (val sample : getCaseSamples(caze)) {
      // TODO: Arbitrary selection of one from many
      return getSampleSubmitterId(sample);
    }

    return null;
  }

  private static String resolveTcgaSampleBarcode(JsonNode caze) {
    for (val sample : getCaseSamples(caze)) {
      for (val portion : getSamplePortions(sample)) {
        for (val analyte : getPortionAnalytes(portion)) {
          for (val aliquot : getAnalyteAliquots(analyte)) {
            // TODO: Arbitrary selection of one from many
            return getAliquotSubmitterId(aliquot);
          }
        }
      }
    }

    return null;
  }

}

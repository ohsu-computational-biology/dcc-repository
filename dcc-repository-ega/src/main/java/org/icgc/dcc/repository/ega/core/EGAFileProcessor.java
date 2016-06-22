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
package org.icgc.dcc.repository.ega.core;

import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.io.Files.getNameWithoutExtension;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Collections.singletonList;
import static org.icgc.dcc.common.core.util.Formats.formatCount;
import static org.icgc.dcc.repository.ega.util.EGAAnalyses.getAnalysisChecksum;
import static org.icgc.dcc.repository.ega.util.EGAAnalyses.getAnalysisFile;
import static org.icgc.dcc.repository.ega.util.EGAAnalyses.getAnalysisFileName;
import static org.icgc.dcc.repository.ega.util.EGAAnalyses.getAnalysisFileType;
import static org.icgc.dcc.repository.ega.util.EGAMappings.getMappingDataSetId;
import static org.icgc.dcc.repository.ega.util.EGAMappings.getMappingFileId;
import static org.icgc.dcc.repository.ega.util.EGAMappings.getMappingFileName;
import static org.icgc.dcc.repository.ega.util.EGAMappings.getMappingFileSize;
import static org.icgc.dcc.repository.ega.util.EGARuns.getRunChecksum;
import static org.icgc.dcc.repository.ega.util.EGARuns.getRunDate;
import static org.icgc.dcc.repository.ega.util.EGARuns.getRunFile;
import static org.icgc.dcc.repository.ega.util.EGARuns.getRunFileName;
import static org.icgc.dcc.repository.ega.util.EGARuns.getRunFileType;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.stream.Stream;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.Repository;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileCopy;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileFormat;
import org.icgc.dcc.repository.ega.model.EGAMetadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EGAFileProcessor extends RepositoryFileProcessor {

  /**
   * Metadata.
   */
  @NonNull
  private final Repository egaRepository;

  /**
   * State.
   */
  private int fileCount = 0;

  public EGAFileProcessor(RepositoryFileContext context, @NonNull Repository egaRepository) {
    super(context);
    this.egaRepository = egaRepository;
  }

  public Stream<RepositoryFile> process(Stream<EGAMetadata> metadata) {
    return metadata.flatMap(this::createFiles);
  }

  private Stream<RepositoryFile> createFiles(EGAMetadata metadata) {
    log.info("Processing dataset {}", metadata.getDatasetId());
    return metadata.getFiles().stream().map(file -> createFile(metadata, file)).filter(file -> file != null);
  }

  private RepositoryFile createFile(EGAMetadata metadata, ObjectNode file) {
    val egaFile = new RepositoryFile();

    egaFile.getDataBundle()
        .setDataBundleId(metadata.getDatasetId());

    val fileCopy = egaFile.addFileCopy()
        .setRepoDataBundleId(metadata.getDatasetId())
        .setRepoFileId(getMappingFileId(file))
        .setRepoDataSetId(getMappingDataSetId(file))
        .setFileSize(getMappingFileSize(file))
        .setFileName(null) // Set from run/analysis later on
        .setRepoType(egaRepository.getType().getId())
        .setRepoOrg(egaRepository.getSource().getId())
        .setRepoName(egaRepository.getName())
        .setRepoCode(egaRepository.getCode())
        .setRepoCountry(egaRepository.getCountry())
        .setRepoBaseUrl(egaRepository.getBaseUrl())
        .setRepoMetadataPath(egaRepository.getType().getMetadataPath())
        .setRepoDataPath(egaRepository.getType().getDataPath());

    updateFileCopy(fileCopy, getMappingFileName(file), metadata);

    if (fileCopy.getFileName() == null) {
      return null;
    }

    // TODO: Filter if not in DCC

    egaFile.addDonor()
        .setProjectCode(getFirst(metadata.getProjectCodes(), null))
        .setSubmittedSampleId(singletonList(resolveSubmittedSampleId(metadata, fileCopy.getRepoFileId())));

    if (++fileCount % 1000 == 0) {
      log.info("Processed {} files", formatCount(fileCount));
    }

    return egaFile;
  }

  private void updateFileCopy(FileCopy fileCopy, String fileName, EGAMetadata metadata) {
    // First try runs
    val runFileNames = Sets.<String> newHashSet();
    {
      val runs = metadata.getMetadata().getRuns().values();
      for (val run : runs) {
        val files = resolveRunFiles(run);

        for (val file : files) {
          val runFile = new File(getRunFileName(file));
          val runFileName = runFile.getName();
          runFileNames.add(runFileName);

          if (isFileNameMatch(fileName, runFileName)) {
            fileCopy
                .setFileName(runFileName)
                .setFileFormat(resolveFileFormat(getRunFileType(file)))
                .setFileMd5sum(getRunChecksum(file))
                .setLastModified(resolveLastModified(getRunDate(run)));

            return;
          }
        }
      }
    }

    // Next try analysis
    val analysisFileNames = Sets.<String> newHashSet();
    {
      val analyses = metadata.getMetadata().getAnalysis().values();
      for (val analysis : analyses) {
        val files = resolveAnalysisFiles(analysis);

        for (val file : files) {
          val analysisFile = new File(getAnalysisFileName(file));
          val analysisFileName = analysisFile.getName();
          analysisFileNames.add(analysisFileName);

          if (isFileNameMatch(fileName, analysisFileName)) {
            fileCopy
                .setFileName(analysisFileName)
                .setFileFormat(resolveFileFormat(getAnalysisFileType(file)))
                .setFileMd5sum(getAnalysisChecksum(file))
                .setLastModified(null); // TODO

            return;
          }
        }
      }
    }

    log.warn("No match for file {} in dataset {} (array-based?: {}) with run file names: {}, analysis file names: {}",
        fileName, metadata.getDatasetId(), isArrayBased(metadata.getDatasetId()), runFileNames, analysisFileNames);
  }

  private static String resolveFileFormat(String fileType) {
    // See http://www.ncbi.nlm.nih.gov/books/NBK47538/#_SRA_DataBlock_BK_sec3_
    if ("bam".equals(fileType)) {
      return FileFormat.BAM;
    }
    if ("fastq".equals(fileType)) {
      return FileFormat.FASTQ;
    }
    if ("srf".equals(fileType)) {
      return FileFormat.SRF;
    }
    if ("cram".equals(fileType)) {
      return FileFormat.CRAM;
    }
    if ("vcf".equals(fileType)) {
      return FileFormat.VCF;
    }

    return fileType;
  }

  private static Long resolveLastModified(String runDate) {
    if (runDate == null) {
      return null;
    }

    val dateTime = LocalDateTime.parse(resolveFileFormat(runDate), ISO_DATE_TIME);

    // EGA location is Barcelona. This is the closet zone id (I could find)
    val egaTimeZone = ZoneId.of("Europe/Madrid");
    return dateTime.atZone(egaTimeZone).toInstant().toEpochMilli();
  }

  private static Iterable<JsonNode> resolveRunFiles(JsonNode root) {
    val values = getRunFile(root);
    return values.isArray() ? values : singletonList(values);
  }

  private static Iterable<JsonNode> resolveAnalysisFiles(JsonNode root) {
    val values = getAnalysisFile(root);
    return values.isArray() ? values : singletonList(values);
  }

  private static String resolveSubmittedSampleId(EGAMetadata metadata, String repoFileId) {
    try {
      return metadata
          .getMetadata()
          .getMappings()
          .get("Sample_File").stream()
          .filter(record -> record.get("FILE_ACCESSION").textValue().equals(repoFileId))
          .findFirst().get()
          .path("SAMPLE_ALIAS").textValue();
    } catch (Exception e) {
      log.warn("Could not resolve submitted sample id for file {} in dataset {}", repoFileId, metadata.getDatasetId());
      return null;
    }
  }

  private static boolean isFileNameMatch(String f1, String f2) {
    // Remove paths
    val x1 = new File(f1).getName();
    val y1 = new File(f2).getName();

    // Remove extension
    val x2 = getNameWithoutExtension(x1);
    val y2 = getNameWithoutExtension(y1);

    return x1.startsWith(y1)
        || x1.startsWith(y2)
        || x2.startsWith(y1)
        || x2.startsWith(y2)

        || y1.startsWith(x1)
        || y1.startsWith(x2)
        || y2.startsWith(x1)
        || y2.startsWith(x2);
  }

  /**
   * Note that AF datasets (containing Array-based files) do not have metadata associated. The EGA only provides the
   * files (normally the metadata is included in these files) for this kind of datasets at the moment.
   * 
   * This is also the case for datasets mentioned in your ticket #528548: EGAD00010000915, EGAD00010000916,
   * EGAD00010000917
   * 
   * Let me share a tip for you to spot the AF datasets (with no metadata associated). EGAD0001* datasets are AF whilst
   * EGAD00001* datasets are sequence/analysis datasets (e.g. https://ega-archive.org/datasets/EGAD00010000238 vs
   * https://ega-archive.org/datasets/EGAD00001002016).
   */
  private static boolean isArrayBased(String datasetId) {
    return datasetId.startsWith("EGAD0001");
  }

}

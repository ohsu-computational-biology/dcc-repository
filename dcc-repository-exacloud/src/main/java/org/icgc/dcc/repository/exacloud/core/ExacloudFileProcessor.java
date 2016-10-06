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
package org.icgc.dcc.repository.exacloud.core;

import static org.icgc.dcc.common.core.util.Formats.formatCount;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.repository.core.model.Repositories.getTCGARepository;
import static org.icgc.dcc.repository.core.model.RepositoryProjects.getProjectByProjectCode;

import java.util.regex.Pattern;

import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileProcessor;
import org.icgc.dcc.repository.core.model.Repository;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.model.RepositoryFile.DataType;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileAccess;
import org.icgc.dcc.repository.core.model.RepositoryFile.FileFormat;
import org.icgc.dcc.repository.core.model.RepositoryFile.OtherIdentifiers;
import org.icgc.dcc.repository.core.model.RepositoryFile.Program;
import org.icgc.dcc.repository.exacloud.model.ExacloudArchiveClinicalFile;
import org.icgc.dcc.repository.exacloud.reader.ExacloudArchiveListReader;

import com.google.common.collect.ImmutableList;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExacloudFileProcessor extends RepositoryFileProcessor {

  /**
   * Constants.
   */
  private static final Pattern CLINICAL_ARCHIVE_NAME_PATTERN = Pattern.compile(".*_(\\w+)\\.bio\\..*");

  /**
   * Metadata.
   */
  @NonNull
  private final Repository tcgaRepository = getTCGARepository();

  public ExacloudFileProcessor(RepositoryFileContext context) {
    super(context);
  }

  public Iterable<RepositoryFile> processClinicalFiles() {
    log.info("Creating clinical files...");
    val clinicalFiles = createClinicalFiles();

    log.info("Filtering clinical files...");
    val filteredClinicalFiles = filterClinicalFiles(clinicalFiles);

//    log.info("Translating TCGA barcodes...");
//    translateBarcodes(filteredClinicalFiles);

    log.info("Assigning study...");
    assignStudy(filteredClinicalFiles);

    return filteredClinicalFiles;
  }

  private Iterable<RepositoryFile> filterClinicalFiles(Iterable<RepositoryFile> clinicalFiles) {
    return stream(clinicalFiles).filter(hasDonorId()).collect(toImmutableList());
  }

  private Iterable<RepositoryFile> createClinicalFiles() {
    log.info("Reading archive list entries...");
    val entries = ExacloudArchiveListReader.readEntries();
    log.info("Read {} archive list entries", formatCount(entries));

    val clinicalFiles = ImmutableList.<RepositoryFile> builder();
    for (val entry : entries) {
//      val matcher = CLINICAL_ARCHIVE_NAME_PATTERN.matcher(entry.getArchiveName());
//      val clinical = matcher.matches();
//      if (!clinical) {
//        continue;
//      }
//
//      val diseaseCode = matcher.group(1);
      val project = getProjectByProjectCode("BAML-US");
//      val unrecognized = !project.isPresent();
//      if (unrecognized) {
//        continue;
//      }

      val archiveClinicalFiles = processArchive(project.get().getProjectCode(), entry.getArchiveUrl());

      clinicalFiles.addAll(archiveClinicalFiles);
    }

    return clinicalFiles.build();
  }

  private Iterable<RepositoryFile> processArchive(String projectCode, String archiveUrl) {
    val processor = new ExacloudArchiveClinicalFileProcessor();
    val archiveClinicalFiles = processor.process(archiveUrl, projectCode);
    log.info("Processing {} archive clinical files", formatCount(archiveClinicalFiles));

    val clinicalFiles = ImmutableList.<RepositoryFile> builder();
    for (val archiveClinicalFile : archiveClinicalFiles) {
      //val clinicalFile = createClinicalFile(projectCode, archiveClinicalFile);

      if (archiveClinicalFile != null) {
        clinicalFiles.add(archiveClinicalFile);
      }
    }

    return clinicalFiles.build();
  }


  private void translateBarcodes(Iterable<RepositoryFile> clinicalFiles) {
    log.info("Collecting TCGA barcodes...");
    val barcodes = stream(clinicalFiles)
        .flatMap(clinicalFile -> clinicalFile.getDonors().stream())
        .map(donor -> donor.getOtherIdentifiers().getTcgaParticipantBarcode())
        .collect(toImmutableSet());

    log.info("Translating {} TCGA barcodes to TCGA UUIDs...", formatCount(barcodes));

    val uuids = context.getTCGAUUIDs(barcodes);
    for (val clinicalFile : clinicalFiles) {
      for (val donor : clinicalFile.getDonors()) {
        val participantBarcode = donor.getOtherIdentifiers().getTcgaParticipantBarcode();

        val uuid = uuids.get(participantBarcode);
        donor.setSubmittedDonorId(uuid);
      }
    }
  }

  private String resolveEntityPath(ExacloudArchiveClinicalFile archiveClinicalFile) {
    return resolvePath(archiveClinicalFile).replace(tcgaRepository.getType().getDataPath(), "");
  }

  private String resolveDataPath(ExacloudArchiveClinicalFile archiveClinicalFile) {
    return resolvePath(archiveClinicalFile).replace(archiveClinicalFile.getFileName(), "");
  }

  private String resolvePath(ExacloudArchiveClinicalFile archiveClinicalFile) {
    val url = archiveClinicalFile.getUrl();
    return url.replace(tcgaRepository.getBaseUrl(), "/");
  }

}

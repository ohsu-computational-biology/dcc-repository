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
package org.icgc.dcc.repository.client.core;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Stopwatch.createStarted;
import static com.google.common.base.Strings.repeat;

import java.util.List;
import java.util.Set;

import org.icgc.dcc.common.core.mail.Mailer;
import org.icgc.dcc.common.core.report.BufferedReport;
import org.icgc.dcc.common.core.report.ReportEmail;
import org.icgc.dcc.repository.aws.AWSImporter;
import org.icgc.dcc.repository.cghub.CGHubImporter;
import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositorySourceFileImporter;
import org.icgc.dcc.repository.core.model.RepositoryFile;
import org.icgc.dcc.repository.core.writer.RepositoryFileWriter;
import org.icgc.dcc.repository.index.core.RepositoryFileIndexer;
import org.icgc.dcc.repository.pcawg.PCAWGImporter;
import org.icgc.dcc.repository.tcga.TCGAImporter;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import lombok.Cleanup;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Importer for the ICGC "Data Repository" feature which imports file metadata from various external data sources.
 * 
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/Uniform+metadata+JSON+document+for+ICGC+Data+Repositories
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/JSON+structure+for+ICGC+data+repository
 * @see https://wiki.oicr.on.ca/display/DCCSOFT/UI+-+The+new+ICGC+DCC+data+repository+-+Simplified+version+Phase+1
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Autowired) )
public class RepositoryImporter {

  /**
   * Import steps
   */
  public enum Step {

    IMPORT,
    MERGE,
    INDEX;

    @Getter(lazy = true)
    @Accessors(fluent = true)
    private static final Set<Step> all = ImmutableSet.copyOf(values());

  }

  /**
   * Dependencies.
   */
  @NonNull
  private final RepositoryFileContext context;
  @NonNull
  private final Mailer mailer;

  @NonNull
  public void execute() {
    execute(Step.all());
  }

  @NonNull
  public void execute(Step... steps) {
    execute(ImmutableSet.copyOf(steps));
  }

  @NonNull
  public void execute(@NonNull Set<Step> steps) {
    val watch = createStarted();

    val exceptions = Lists.<Exception> newArrayList();
    try {

      //
      // Import
      //

      if (!steps.contains(Step.IMPORT)) {
        log.warn("*** Skipping import!");
      } else {
        // Write and always continue if an exception
        exceptions.addAll(writeSourceFiles());
      }

      //
      // Merge
      //

      if (!steps.contains(Step.MERGE)) {
        log.warn("*** Skipping merge!");
      } else {
        // Collect
        val files = collectFiles();

        // Aggregate
        val combinedFiles = combineFiles(files);

        // Aggregate
        val filteredFiles = filterFiles(combinedFiles);

        // Write
        writeFiles(filteredFiles);
      }

      //
      // Index
      //

      if (!steps.contains(Step.INDEX)) {
        log.warn("*** Skipping merge!");
      } else {
        // Index
        indexFiles();
      }
    } catch (Exception e) {
      exceptions.add(e);
    } finally {
      report(watch, exceptions);
    }

    checkState(exceptions.isEmpty(), "Exception(s) processing %s", exceptions);
  }

  private List<Exception> writeSourceFiles() {
    val importers = createImporters(context);

    val exceptions = ImmutableList.<Exception> builder();
    for (val importer : importers) {
      boolean active = context.isSourceActive(importer.getSource());
      if (active) {
        try {
          logBanner("Importing '" + importer.getSource() + "' sourced files");
          importer.execute();
        } catch (Exception e) {
          log.error("Error procesing '" + importer.getSource() + "': ", e);
          exceptions.add(e);
        }
      }
    }

    return exceptions.build();
  }

  private Iterable<Set<RepositoryFile>> collectFiles() {
    logBanner("Collecting files");
    val collector = new RepositoryFileCollector(context);
    return collector.collectFiles();
  }

  private Iterable<RepositoryFile> combineFiles(Iterable<Set<RepositoryFile>> files) {
    logBanner("Combining files");
    val combiner = new RepositoryFileCombiner(context);
    return combiner.combineFiles(files);
  }

  private Iterable<RepositoryFile> filterFiles(Iterable<RepositoryFile> files) {
    logBanner("Filtering files");
    val filter = new RepositoryFileFilter(context);
    return filter.filterFiles(files);
  }

  @SneakyThrows
  private void writeFiles(Iterable<RepositoryFile> files) {
    logBanner("Writing files");
    @Cleanup
    val writer = new RepositoryFileWriter(context.getMongoUri());
    writer.write(files);
  }

  @SneakyThrows
  private void indexFiles() {
    logBanner("Indexing files");
    @Cleanup
    val indexer = new RepositoryFileIndexer(context.getMongoUri(), context.getEsUri());
    indexer.indexFiles();
  }

  private void report(Stopwatch watch, List<Exception> exceptions) {
    val success = exceptions.isEmpty();
    if (success) {
      log.info("Finished importing repository in {}", watch);
    } else {
      log.error("Finished importing repository with errors in {}:", watch);
      int i = 0;
      for (val e : exceptions) {
        log.error("[" + ++i + "/" + exceptions.size() + "]: ", e);
      }
    }

    val report = new BufferedReport();
    report.addTimer(watch);

    for (val exception : exceptions) {
      report.addException(exception);
    }

    val message = new ReportEmail("DCC Repository Importer", report);
    mailer.sendMail(message);
  }

  private static List<RepositorySourceFileImporter> createImporters(RepositoryFileContext context) {
    // Order will be execution order subject to activation
    return ImmutableList.of(
        new PCAWGImporter(context),
        new TCGAImporter(context),
        new CGHubImporter(context),
        new AWSImporter(context));
  }

  private static void logBanner(String message) {
    log.info(repeat("-", 80));
    log.info(message);
    log.info(repeat("-", 80));
  }

}

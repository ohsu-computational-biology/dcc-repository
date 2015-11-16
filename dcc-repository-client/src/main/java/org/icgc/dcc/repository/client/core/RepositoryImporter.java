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
import org.icgc.dcc.common.core.report.ReportEmail;
import org.icgc.dcc.repository.aws.AWSImporter;
import org.icgc.dcc.repository.cghub.CGHubImporter;
import org.icgc.dcc.repository.collab.CollabImporter;
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
    log.info("Running steps {} using sources {}", steps, context.getSources());

    int stepNumber = 1;
    val stepCount = steps.size();
    val watch = createStarted();
    try {

      //
      // Import
      //

      if (!steps.contains(Step.IMPORT)) {
        log.warn("*** Skipping import!");
      } else {
        // Write and always continue if an exception
        logStep(stepNumber++, stepCount, "Importing sources");
        writeSourceFiles();
      }

      //
      // Merge
      //

      if (!steps.contains(Step.MERGE)) {
        log.warn("*** Skipping merge!");
      } else {
        logStep(stepNumber++, stepCount, "Merging files");

        // Collect
        val files = collectFiles();

        // Combine
        val combinedFiles = combineFiles(files);

        // Filter
        val filteredFiles = filterFiles(combinedFiles);

        // Write
        writeFiles(filteredFiles);
      }

      //
      // Index
      //

      if (!steps.contains(Step.INDEX)) {
        log.warn("*** Skipping index!");
      } else {
        // Index
        logStep(stepNumber++, stepCount, "Indexing files");
        indexFiles();
      }
    } catch (Exception e) {
      reportException("Unknown exception processing", e);
    } finally {
      report(watch);
    }

    val exceptions = context.getReport().getExceptions();
    checkState(exceptions.isEmpty(), "Exception(s) processing %s", exceptions);
  }

  private void writeSourceFiles() {
    val importers = createImporters(context);

    int sourceNumber = 1;
    val sourceCount = context.getSources().size();
    for (val importer : importers) {
      val active = context.isSourceActive(importer.getSource());
      if (active) {
        try {
          log.info(repeat("-", 80));
          log.info("[{}/{}] Import: {}", sourceNumber++, sourceCount, importer.getSource());
          log.info(repeat("-", 80));

          // Perform import of source
          importer.execute();
        } catch (Exception e) {
          reportException(String.format("Error processing '%s': %s", importer.getSource(), e.getMessage()), e);
        }
      }
    }
  }

  private void reportException(final java.lang.String message, Exception e) {
    log.error(message, e);
    context.getReport().addError(message);
    context.getReport().addException(e);
  }

  private Iterable<Set<RepositoryFile>> collectFiles() {
    val collector = new RepositoryFileCollector(context);
    return collector.collectFiles();
  }

  private Iterable<RepositoryFile> combineFiles(Iterable<Set<RepositoryFile>> files) {
    val combiner = new RepositoryFileCombiner(context);
    return combiner.combineFiles(files);
  }

  private Iterable<RepositoryFile> filterFiles(Iterable<RepositoryFile> files) {
    val filter = new RepositoryFileFilter(context);
    return filter.filterFiles(files);
  }

  @SneakyThrows
  private void writeFiles(Iterable<RepositoryFile> files) {
    @Cleanup
    val writer = new RepositoryFileWriter(context.getMongoUri());
    writer.write(files);
  }

  @SneakyThrows
  private void indexFiles() {
    @Cleanup
    val indexer = new RepositoryFileIndexer(context.getMongoUri(), context.getEsUri(), context.getIndexAlias());
    indexer.indexFiles();
  }

  private void report(Stopwatch watch) {
    val report = context.getReport();
    report.addTimer(watch);

    val success = report.getExceptionCount() == 0;
    if (success) {
      log.info("Finished importing repositories in {}", watch);
    } else {
      log.error("Finished importing repositories with errors in {}:", watch);
      int i = 0;
      for (val e : report.getExceptions()) {
        log.error("[" + ++i + "/" + report.getExceptions().size() + "]: ", e);
      }
    }

    val message = new ReportEmail("DCC Repository", report);
    mailer.sendMail(message);
  }

  private static List<RepositorySourceFileImporter> createImporters(RepositoryFileContext context) {
    // The list order will be execution order, subject to activation
    return ImmutableList.of(
        new PCAWGImporter(context),
        new AWSImporter(context),
        new CollabImporter(context),
        new TCGAImporter(context),
        new CGHubImporter(context));
  }

  private static void logStep(int stepNumber, int stepCount, String message) {
    log.info(repeat("#", 80));
    log.info("[{}/{}] Step: " + message, stepNumber, stepCount);
    log.info(repeat("#", 80));
  }

}

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
package org.icgc.dcc.repository.client.config;

import org.icgc.dcc.common.core.mail.Mailer;
import org.icgc.dcc.common.core.report.BufferedReport;
import org.icgc.dcc.repository.client.core.RepositoryImporter;
import org.icgc.dcc.repository.core.RepositoryFileContext;
import org.icgc.dcc.repository.core.RepositoryFileContextBuilder;
import org.icgc.dcc.repository.core.util.DCCDonorIdResolver;
import org.icgc.dcc.repository.pcawg.core.PCAWGDonorIdResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import lombok.val;

@Configuration
public class ClientConfig {

  @Bean
  public RepositoryImporter importer(RepositoryFileContext context, Mailer mailer) {
    return new RepositoryImporter(context, mailer);
  }

  @Bean
  public Mailer mailer(ClientProperties properties) {
    val mailConfig = properties.getMail();

    return Mailer.builder()
        .enabled(mailConfig.isEnabled())
        .host(mailConfig.getSmtpServer())
        .recipient(mailConfig.getRecipients())
        .build();
  }

  @Bean
  @DependsOn("clientBanner")
  public RepositoryFileContext context(ClientProperties properties) {
    val context = RepositoryFileContextBuilder.builder();

    // Inputs
    context
        .sources(properties.getRepository().getSources());

    // IDs
    context
        .idUrl(properties.getId().getServiceUrl())
        .authToken(properties.getId().getAuthToken())
        .realIds(true);

    // Reference
    context
        .pcawgIdResolver(new PCAWGDonorIdResolver())
        .dccIdResolver(new DCCDonorIdResolver())
        .importMongoUri(properties.getImports().getMongoUri());

    // Outputs
    context
        .repoMongoUri(properties.getRepository().getMongoUri())
        .esUri(properties.getRepository().getEsUri())
        .archiveUri(properties.getRepository().getArchiveUri())
        .indexAlias(properties.getRepository().getIndexAlias());

    // Reporting
    context
        .report(new BufferedReport());

    return context.build();
  }

}

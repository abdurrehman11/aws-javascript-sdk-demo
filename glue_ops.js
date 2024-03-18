import { GlueClient, StartCrawlerCommand, StartJobRunCommand } from "@aws-sdk/client-glue"; // ES Modules import

const client = new GlueClient();

const crawlerInput = { 
  Name: "employee_crawler",
}

const jobInput = { 
    JobName: "glue_employee_job",
}
  

async function startGlueCrawler() {
    try {
        const command = new StartCrawlerCommand(crawlerInput);
        const response = await client.send(command);
        console.log('Crawler Execution Started');
    } catch (error) {
        console.error('Crawler Execution Failed:', error);
    }
}

async function startGlueJob() {
    try {
        const command = new StartJobRunCommand(jobInput);
        const response = await client.send(command);
        console.log('Glue Job Execution Started');
    } catch (error) {
        console.error('Glue Job Execution Failed:', error);
    }
}

async function main() {
    const args = process.argv.slice(2); // Remove the first two arguments (node executable and script name)
    const [action] = args;
  
    if (action === 'crawl') {
      await startGlueCrawler();
    } else if (action == 'run-job') {
      await startGlueJob();
    } else {
      console.error(`Invalid action: ${action}. Please specify 'crawl'', or 'run-job'.`);
    }
  }
  
  main();
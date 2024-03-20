import { CloudWatchLogsClient, CreateExportTaskCommand } from "@aws-sdk/client-cloudwatch-logs"; 

const client = new CloudWatchLogsClient();

// const NDAYS = 1;
// const nDays = parseInt(NDAYS);

// const currentTime = new Date();
// const StartDate = new Date(currentTime.getTime() - nDays * 24 * 60 * 60 * 1000);
// const EndDate = new Date(currentTime.getTime() - (nDays - 1) * 24 * 60 * 60 * 1000);

// console.log("===", StartDate, EndDate);

// const fromDate = Math.floor(StartDate.getTime() / 1000);
// const toDate = Math.floor(EndDate.getTime() / 1000);

// console.log("fromDate:", fromDate, "toDate:", toDate);

const startLogTimestamp = "2024-03-19T20:30:45.557+05:00";
const endLogTimestamp = "2024-03-19T21:30:45.557+05:00";


// Parse the timestamp string to create a Date object
const startlogDate = new Date(startLogTimestamp);
const endlogDate = new Date(endLogTimestamp);

const startDateNumber = Number(startlogDate.getTime());
const endDateNumber = Number(endlogDate.getTime());

console.log(typeof startDateNumber === 'number'); // Output: true
console.log(startDateNumber instanceof Number, endDateNumber);

// Calculate the start and end dates based on the log timestamp
// In this example, we'll create a time range of 1 hour
// const startDate = new Date(logDate);
// const endDate = new Date(logDate.getTime() + (1 * 60 * 60 * 1000)); // Add 1 hour to the log timestamp

// console.log("Start Date:", startDate.toISOString(), logDate.getTime());
// console.log("End Date:", endDate.toISOString());

// const fromDate = Math.floor(startDate.getTime() / 1000);
// const toDate = Math.floor(endDate.getTime() / 1000);

// console.log("fromDate:", fromDate, "toDate:", toDate, "======", startDate.getTime());

const DESTINATION_BUCKET = "ar-test-cloudwatch-logs";
const GROUP_NAME = "/aws/lambda/cloudwatch-logs-generator";
const TASK_NAME = "write-clw-logs-to-s3";
const PREFIX = "CloudQuickLabs";
// const BUCKET_PREFIX = `${PREFIX}/${StartDate.getFullYear()}/${(StartDate.getMonth() + 1).toString().padStart(2, '0')}/${StartDate.getDate().toString().padStart(2, '0')}`;

// console.log("BUCKET_PREFIX: ", BUCKET_PREFIX);

const input = { 
  taskName: TASK_NAME,
  logGroupName: GROUP_NAME, 
  from: startDateNumber,
  to: endDateNumber,
  destination: DESTINATION_BUCKET, 
  destinationPrefix: PREFIX,
};

async function sendCLWlogToS3() {
    try {
        const command = new CreateExportTaskCommand(input);
        const response = await client.send(command);
        console.log('CloudWatch Export Task Started!');
    } catch (error) {
        console.error('CloudWatch Export Task Failed:', error);
    }
};


async function main() {
    await sendCLWlogToS3();
}


main();


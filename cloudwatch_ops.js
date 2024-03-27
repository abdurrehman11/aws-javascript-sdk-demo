import { CloudWatchLogsClient, CreateExportTaskCommand } from "@aws-sdk/client-cloudwatch-logs"; 

const client = new CloudWatchLogsClient();

const startLogTimestamp = "2024-03-19T20:30:45.557+05:00";
const endLogTimestamp = "2024-03-19T21:30:45.557+05:00";


// Parse the timestamp string to create a Date object
const startlogDate = new Date(startLogTimestamp);
const endlogDate = new Date(endLogTimestamp);

const startDateNumber = Number(startlogDate.getTime());
const endDateNumber = Number(endlogDate.getTime());

console.log(typeof startDateNumber === 'number'); // Output: true
console.log(startDateNumber instanceof Number, endDateNumber);

const DESTINATION_BUCKET = "ar-test-cloudwatch-logs";
const GROUP_NAME = "/aws/lambda/cloudwatch-logs-generator";
const TASK_NAME = "write-clw-logs-to-s3";
const PREFIX = "test-logs";

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


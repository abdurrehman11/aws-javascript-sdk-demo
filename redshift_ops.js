import { RedshiftDataClient, ExecuteStatementCommand } from "@aws-sdk/client-redshift-data";

// Replace all the constant as per your created attributes
const REGION = 'us-east-1';
const DATABASE = 'dev';
const WORKGROUP_NAME = 'default-workgroup-101';
const IAM_ROLE_ARN = 'arn:aws:iam::471112663332:role/s3-to-redshift-role';
const S3_BUCKET = 's3-cdk-bucket-ar';
const EMPLOYEE_JSON_PATH = `s3://${S3_BUCKET}/json/employee.json`;
const EMPLOYEE_JSONPATH_JSON = `s3://${S3_BUCKET}/jsonpaths/employee_jsonpath.json`;
const EMPLOYEE_REDSHIFT_JSON = `s3://${S3_BUCKET}/json/employee_redshift.json`;
const KMS_KEY_ID = '';

async function executeSql(sql) {
  const client = new RedshiftDataClient({ region: REGION });

  const input = {
    Sql: sql,
    Database: DATABASE,
    WorkgroupName: WORKGROUP_NAME,
  };

  const command = new ExecuteStatementCommand(input);

  try {
    const response = await client.send(command);
    console.log("Query executed successfully:", response);
    return response;
  } catch (error) {
    console.error("Error executing query:", error);
    throw error;
  }
}

async function createRedshiftTable() {
  try {
    const sql_query = "CREATE TABLE employee(emp_id INT, emp_name VARCHAR(50), emp_dept VARCHAR(50), primary key(emp_id));";
    await executeSql(sql_query);
  } catch (error) {
    console.error("An error occurred:", error);
  }
}

async function loadToRedshiftFromS3() {
  try {
    const sql_query = `
      COPY employee FROM '${EMPLOYEE_JSON_PATH}'
      REGION '${REGION}' 
      IAM_ROLE '${IAM_ROLE_ARN}'
      FORMAT JSON '${EMPLOYEE_JSONPATH_JSON}';
    `;

    await executeSql(sql_query);
  } catch (error) {
    console.error("An error occurred:", error);
  }
}

async function unloadFromRedshiftToS3() {
  try {
    const sql_query = `
      UNLOAD ('select emp_id, emp_name, emp_dept from employee')
      TO '${EMPLOYEE_REDSHIFT_JSON}'
      REGION '${REGION}'
      IAM_ROLE '${IAM_ROLE_ARN}'
      KMS_KEY_ID '${KMS_KEY_ID}'
      ENCRYPTED
      PARALLEL OFF;
    `;

    await executeSql(sql_query);
  } catch (error) {
    console.error("An error occurred:", error);
  }
}

async function main() {
  const args = process.argv.slice(2); // Remove the first two arguments (node executable and script name)
  const [action] = args;

  if (action === 'create') {
    await createRedshiftTable();
  } else if (action === 'load') {
    await loadToRedshiftFromS3();
  } else if (action === 'unload') {
    await unloadFromRedshiftToS3();
  } else {
    console.error(`Invalid action: ${action}. Please specify 'create', 'load', or 'unload'.`);
  }
}

main();


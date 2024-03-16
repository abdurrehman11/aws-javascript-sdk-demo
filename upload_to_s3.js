import { promises as fs } from 'fs';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';


// Replace all the constant as per your created attributes
const REGION = 'us-east-1';
const BUCKET_NAME = 's3-cdk-bucket-ar';
const DATA_DIR = './data/';
const JSON_FILE = 'employee.json';
const JSON_PATH_FILE = 'employee_jsonpath.json';

const s3Client = new S3Client({ region: REGION });

async function uploadFileToS3(bucketName, key, filePath) {
  try {
    const fileContent = await fs.readFile(filePath, 'utf-8');
    const params = {
      Bucket: bucketName,
      Key: key,
      Body: fileContent,
    };
    const command = new PutObjectCommand(params);
    const data = await s3Client.send(command);
    console.log('File uploaded successfully:', data);
  } catch (error) {
    console.error('Error uploading file:', error);
  }
}

const jsonDataKey = `json/${JSON_FILE}`;
const jsonDataFilePath = `${DATA_DIR}${JSON_FILE}`;

const jsonSchemaDataKey = `jsonpaths/${JSON_PATH_FILE}`;
const jsonSchemaFilePath = `${DATA_DIR}${JSON_PATH_FILE}`;

uploadFileToS3(BUCKET_NAME, jsonDataKey, jsonDataFilePath);
uploadFileToS3(BUCKET_NAME, jsonSchemaDataKey, jsonSchemaFilePath);

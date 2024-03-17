import { promises as fs } from 'fs';
import { S3Client, PutObjectCommand, ListObjectVersionsCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';


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

async function uploadDataToS3() {
    uploadFileToS3(BUCKET_NAME, jsonDataKey, jsonDataFilePath);
    uploadFileToS3(BUCKET_NAME, jsonSchemaDataKey, jsonSchemaFilePath);
}

async function deleteDataFromS3Versioned() {
  try {
    // List all object versions
    const listObjectVersionsParams = { Bucket: BUCKET_NAME };
    const listObjectVersionsCommand = new ListObjectVersionsCommand(listObjectVersionsParams);
    const listObjectVersionsResponse = await s3Client.send(listObjectVersionsCommand);

    // Delete all object versions
    const deleteVersionsPromises = listObjectVersionsResponse.Versions.map(version => {
        const deleteParams = {
            Bucket: BUCKET_NAME,
            Key: version.Key,
            VersionId: version.VersionId
        };
        const deleteCommand = new DeleteObjectCommand(deleteParams);
        return s3Client.send(deleteCommand);
    });

    // Check if delete markers exist
    if (listObjectVersionsResponse.DeleteMarkers) {

      // Delete all delete markers
      const deleteMarkersPromises = listObjectVersionsResponse.DeleteMarkers.map(marker => {
          const deleteMarkerParams = {
              Bucket: BUCKET_NAME,
              Key: marker.Key,
              VersionId: marker.VersionId
          };
          const deleteMarkerCommand = new DeleteObjectCommand(deleteMarkerParams);
          return s3Client.send(deleteMarkerCommand);
      });

      // Wait for all delete markers to be deleted
      await Promise.all(deleteMarkersPromises);
    }
    
    // Wait for all object versions to be deleted
    await Promise.all(deleteVersionsPromises);

    console.log('All objects and delete markers have been deleted.');
  } catch (error) {
      console.error('Error deleting objects:', error);
  }
}


async function main() {
  const args = process.argv.slice(2); // Remove the first two arguments (node executable and script name)
  const [action] = args;

  if (action === 'upload') {
    await uploadDataToS3();
  } else if (action === 'delete') {
    await deleteDataFromS3Versioned();
  } else {
    console.error(`Invalid action: ${action}. Please specify 'upload'', or 'delete'.`);
  }
}

main();
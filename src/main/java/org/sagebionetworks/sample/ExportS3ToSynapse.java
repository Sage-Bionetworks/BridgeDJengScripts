package org.sagebionetworks.sample;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.client.SynapseClientImpl;
import org.sagebionetworks.client.exceptions.SynapseException;
import org.sagebionetworks.repo.model.FileEntity;
import org.sagebionetworks.repo.model.Folder;
import org.sagebionetworks.repo.model.file.S3FileHandle;
import org.sagebionetworks.repo.model.file.UploadType;
import org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting;
import org.sagebionetworks.repo.model.project.ProjectSettingsType;
import org.sagebionetworks.repo.model.project.UploadDestinationListSetting;

/**
 * This sample assumes you know how to get your data into S3 and how to create a Synapse project. When you run this
 * sample code, it will take all the files in the specified S3 bucket under the specified S3 subfolder and export them
 * to Synapse using the same folder hierarchy.
 *
 * Maven dependencies:
 * <dependency>
 *     <groupId>com.amazonaws</groupId>
 *     <artifactId>aws-java-sdk-dynamodb</artifactId>
 *     <version>1.11.714</version>
 * </dependency>
 * <dependency>
 *     <groupId>commons-codec</groupId>
 *     <artifactId>commons-codec</artifactId>
 *     <version>1.13</version>
 * </dependency>
 * <dependency>
 *     <groupId>org.sagebionetworks</groupId>
 *     <artifactId>synapseJavaClient</artifactId>
 *     <version>294.0</version>
 * </dependency>
 */
public class ExportS3ToSynapse {
    private static final String DELIMITER = "/";

    private final DigestUtils md5DigestUtils;
    private final AmazonS3 s3Client;
    private final String s3Bucket;
    private final String s3Folder;
    private final SynapseClient synapseClient;
    private final String synapseParentId;

    private Long storageLocationId;

    public ExportS3ToSynapse(String awsAccessKey, String awsSecretKey, String synapseUsername, String synapseApiKey,
            String s3Bucket, String s3Folder, String synapseParentId) {
        // Init MD5 digest utils.
        this.md5DigestUtils = new DigestUtils(DigestUtils.getMd5Digest());

        // Init S3 Client.
        AWSCredentials awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
        s3Client = AmazonS3ClientBuilder.standard().withCredentials(awsCredentialsProvider).build();

        // Init Synapse Client.
        synapseClient = new SynapseClientImpl();
        synapseClient.setUsername(synapseUsername);
        synapseClient.setApiKey(synapseApiKey);

        // This is the S3 bucket and folder that we import from. If folder is blank, we import all files in the S3
        // bucket.
        this.s3Bucket = s3Bucket;
        this.s3Folder = s3Folder;

        // This is the parent entity in Synapse in which we export the S3 files. This can be a project or a folder.
        this.synapseParentId = synapseParentId;
    }

    /**
     * Initializes the given Synapse parent (project or folder). Specifically, we need to create a storage location
     * that allows Synapse files from external S3 buckets.
     *
     * This doesn't have to be done every time you export files to Synapse. Once you set this up once (for a particular
     * Synapse project and S3 bucket), it's available for use for future export tasks.
     *
     * Some setup will need to be done on the AWS side to make this work. For more information, see
     * https://docs.synapse.org/articles/custom_storage_location.html
     */
    public void initialize() throws SynapseException {
        // In order to prove you own the bucket, the bucket must contain a file called owner.txt that contains your
        // Synapse username. This file must live in the specified folder (or root, if the folder was not specified).
        // This file does not have to be created programmatically when you export files. It can be created as part of
        // bucket setup. It is included in this export sample code to demonstrate what needs to be done.
        byte[] ownerTxtContent = synapseClient.getUserName().getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream ownerTxtInputStream = new ByteArrayInputStream(ownerTxtContent);
        String ownerTxtS3Key;
        if (s3Folder != null) {
            ownerTxtS3Key = s3Folder + "/owner.txt";
        } else {
            ownerTxtS3Key = "owner.txt";
        }
        s3Client.putObject(s3Bucket, ownerTxtS3Key, ownerTxtInputStream, null);

        // Create storage location. This lets Synapse know that we want to import files for this S3 bucket.
        ExternalS3StorageLocationSetting storageLocation = new ExternalS3StorageLocationSetting();
        storageLocation.setBaseKey(s3Folder);
        storageLocation.setUploadType(UploadType.S3);
        storageLocation.setBucket(s3Bucket);
        storageLocation = synapseClient.createStorageLocationSetting(storageLocation);
        storageLocationId = storageLocation.getStorageLocationId();

        // Create the Upload Destination List Settings. This applies the storage location to a project or folder.
        // Note that even though the attribute is called "ProjectId", it takes in either a project ID or a folder ID.
        UploadDestinationListSetting setting = (UploadDestinationListSetting) synapseClient.getProjectSetting(
                synapseParentId, ProjectSettingsType.upload);
        if (setting != null) {
            setting.setLocations(Collections.singletonList(storageLocationId));
            synapseClient.updateProjectSetting(setting);
        } else {
            setting = new UploadDestinationListSetting();
            setting.setLocations(Collections.singletonList(storageLocationId));
            setting.setSettingsType(ProjectSettingsType.upload);
            setting.setProjectId(synapseParentId);
            synapseClient.createProjectSetting(setting);
        }
    }

    /** Executes the export. */
    public void execute() throws IOException, SynapseException {
        executeForSubfolder(s3Folder, synapseParentId);
    }

    /**
     * Helper method to recursively walk the S3 folder hierarchy and export files to the given Synapse parent entity.
     * When this method makes a recursive call, it will update s3SubFolder to the next folder and it will create a
     * Synapse folder in which to export S3 files.
     *
     * @param s3SubFolder
     *         the S3 subfolder from which we export files; if this is null then we export from the S3 bucket root
     * @param synapseEntityId
     *         the Synapse entity in which we export the S3 files; can be a project or a folder
     */
    private void executeForSubfolder(String s3SubFolder, String synapseEntityId) throws IOException, SynapseException {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(s3Bucket)
                .withPrefix(s3SubFolder).withDelimiter(DELIMITER);
        ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
        boolean hasNext;
        do {
            // Export all files in the listing.
            List<S3ObjectSummary> objectSummaryList = objectListing.getObjectSummaries();
            if (objectSummaryList != null) {
                for (S3ObjectSummary objectSummary : objectSummaryList) {
                    String s3Key = objectSummary.getKey();
                    if (s3Key.equals(s3SubFolder)) {
                        // S3 does this weird thing where if you list objects inside of a folder, one of the results
                        // is the folder itself. Creating folders is handled in a different loop (common prefixes,
                        // below), so just skip this one.
                        continue;
                    }

                    exportOneFile(objectSummary.getBucketName(), objectSummary.getKey(), synapseEntityId);
                }
            }

            // Recursively call for all subfolders in the listing.
            List<String> childFolderList = objectListing.getCommonPrefixes();
            if (childFolderList != null) {
                for (String childFolder : childFolderList) {
                    String relativeChildFolder = childFolder;
                    if (s3SubFolder != null) {
                        // Subfolder is an absolute path from the root of the S3 bucket. Extract relative subfolder.
                        relativeChildFolder = childFolder.substring(s3SubFolder.length());
                    }
                    String[] relativeChildFolderTokens = relativeChildFolder.split(DELIMITER);

                    // Make Synapse folders.
                    String previousParentId = synapseEntityId;
                    for (String folderToken : relativeChildFolderTokens) {
                        if (folderToken == null || folderToken.isEmpty()) {
                            // This could happen if there are some leading or trailing slashes.
                            continue;
                        }

                        Folder folder = new Folder();
                        folder.setName(folderToken);
                        folder.setParentId(previousParentId);
                        folder = synapseClient.createEntity(folder);
                        previousParentId = folder.getId();
                    }

                    // Recursively call for the subfolder.
                    executeForSubfolder(childFolder, previousParentId);
                }
            }

            // Fetch next page, if it exists.
            hasNext = objectListing.isTruncated();
            if (hasNext) {
                objectListing = s3Client.listNextBatchOfObjects(objectListing);
            }
        } while (hasNext);
    }

    /**
     * Helper method which exports one S3 file in the specified bucket and key and exports it to the specified
     * Synapse entity.
     *
     * @param s3Bucket
     *         the bucket that the S3 file lives in
     * @param s3Key
     *         the absolute path of the S3 file to export
     * @param synapseParentId
     *         the Synapse entity (project or folder) that the S3 file should be exported to.
     */
    private void exportOneFile(String s3Bucket, String s3Key, String synapseParentId) throws IOException,
            SynapseException {
        // If you don't specify a file name, Synapse might generate a rather user-unfriendly one for you. We should
        // extract the filename from the S3 key.
        String filename = s3Key;
        if (s3Key.contains(DELIMITER)) {
            // Key is a path. Just get the leaf.
            filename = s3Key.substring(s3Key.lastIndexOf(DELIMITER) + 1);
        }

        // Synapse requires external file handles to provide the MD5 hash, for file validation. In this sample code, we
        // download the file from S3 to hash it. We can save a file download by pre-computing the MD5 before export and
        // storing that MD5 hash somewhere (such as in S3 metadata). If we do this, we can export the file to Synapse
        // with no file transfer.
        S3Object s3Object = s3Client.getObject(s3Bucket, s3Key);
        byte[] md5 = md5DigestUtils.digest(s3Object.getObjectContent());
        String md5HexEncoded = Hex.encodeHexString(md5);

        // Create the file handle. This tells Synapse that the file exists and where to find it.
        S3FileHandle fileHandle = new S3FileHandle();
        fileHandle.setFileName(filename);
        fileHandle.setBucketName(s3Bucket);
        fileHandle.setKey(s3Key);
        fileHandle.setContentMd5(md5HexEncoded);
        fileHandle.setStorageLocationId(storageLocationId);
        fileHandle = synapseClient.createExternalS3FileHandle(fileHandle);

        // Now create the file entity. This puts the file in the Synapse hierarchy, where you can access it through
        // Synapse, set permissions, and share with others.
        FileEntity fileEntity = new FileEntity();
        fileEntity.setName(filename);
        fileEntity.setDataFileHandleId(fileHandle.getId());
        fileEntity.setParentId(synapseParentId);
        synapseClient.createEntity(fileEntity);
    }
}

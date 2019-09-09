using System;
using System.Configuration;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using ValidateCopiedFileLength;
using System.IO;

namespace FetchBlobFileInfo
{
    class Program
    {
        static string connectionString = ConfigurationManager.AppSettings["connectionString"];
        static string containerName = ConfigurationManager.AppSettings["containerName"];
        static string path = ConfigurationManager.AppSettings["path"];
        private static string specifiedContinuationTokenStr = ConfigurationManager.AppSettings["continuationToken"];

        private const string OutputPath = Constants.FetchBlobFileInfo + "/" + Constants.OutputPath;
        private const string LogPath = Constants.FetchBlobFileInfo + "/" + Constants.LogPath;

        static void Main(string[] args)
        {
            var now = DateTime.Now.ToString("yyyy_MM_dd HH_mm_ss");
            var outputFilePath = Helper.PathCombine(OutputPath, "Output_" + now + ".txt");
            var logFilePath = Helper.PathCombine(LogPath, "Log_" + now + ".txt");

            try
            {

                Helper.InitDirectory(OutputPath);
                Helper.InitDirectory(LogPath);



                BlobContinuationToken continuationToken = string.IsNullOrWhiteSpace(specifiedContinuationTokenStr)
                    ? null
                    : JsonConvert.DeserializeObject<BlobContinuationToken>(specifiedContinuationTokenStr);

                var account = CloudStorageAccount.Parse(connectionString);
                var client = account.CreateCloudBlobClient();

                long fileCount = 0;
                do
                {

                    var container = client.GetContainerReference(containerName);
                    var directory = container.GetDirectoryReference(path);
                    var blobResult = directory.ListBlobsSegmented(true, BlobListingDetails.None, null,
                        continuationToken, null, null);

                    continuationToken = blobResult.ContinuationToken;
                    var continuationTokenStr = continuationToken == null
                        ? null
                        : JsonConvert.SerializeObject(blobResult.ContinuationToken);
                    var fileItemInfos = ToFileItemInfos(blobResult.Results, Helper.PathCombine(containerName, path))
                        .ToList();
                    fileCount += fileItemInfos.Count;

                    File.AppendAllLines(outputFilePath, fileItemInfos.ToLines());
                    File.AppendAllLines(logFilePath,
                        new string[]
                        {
                            string.Format(CultureInfo.InvariantCulture,
                                "Fetched Blob file count: {0}. Continuation token: '{1}'.", fileCount,
                                continuationTokenStr)
                        });
                    Console.WriteLine("Fetched Blob file count: " + fileCount);

                } while (continuationToken != null);

                File.AppendAllLines(logFilePath,
                    new string[] { "Fetch Blob file info completed. Total file count: " + fileCount });
                Console.WriteLine("Fetch Blob file info completed. Total file count: " + fileCount);
            }
            catch (Exception e)
            {
                File.AppendAllLines(logFilePath,
                    new string[] {"Fetch Blob file info failed: " + Helper.GetLogString(e, true, ExtractMsg)});
                Console.WriteLine("Fetch Blob file info failed: " + Helper.GetLogString(e, true, ExtractMsg));
                throw;
            }
        }


        private static IEnumerable<FileItemInfo> ToFileItemInfos(IEnumerable<IListBlobItem> blobs, string basePath)
        {
            foreach (var blob in blobs)
            {
                var element = new FileItemInfo()
                {
                    Name = Helper.RemoveBasePath(blob.Uri.LocalPath, basePath)
                };

                ICloudBlob icloudBlob = blob as ICloudBlob;
                if (icloudBlob != null)
                {
                    element.Length = icloudBlob.Properties.Length;
                    element.LastModifiedUtc = icloudBlob.Properties.LastModified.Value.UtcDateTime;
                }
                else
                {
                    CloudBlob cloudBlob = blob as CloudBlob;
                    if (cloudBlob != null)
                    {
                        element.Length = cloudBlob.Properties.Length;
                        element.LastModifiedUtc = cloudBlob.Properties.LastModified.Value.UtcDateTime;
                    }
                }
                
                if(element.Length != null)
                    yield return element;
            }
        }

        private static String ExtractMsg(Exception exception)
        {
            StorageException storageEx = exception as StorageException;

            if (storageEx != null)
            {
                var message = storageEx.RequestInformation != null && storageEx.RequestInformation.ExtendedErrorInformation != null ? storageEx.RequestInformation.ExtendedErrorInformation.ErrorMessage : null;

                if (message != null)
                {
                    // if this is a StorageClientException, log the extended error message which contains request id.
                    return String.Format(CultureInfo.InvariantCulture, "{0}={1},", "StorageExtendedMessage", message);
                }
            }

            return null;
        }
    }
}

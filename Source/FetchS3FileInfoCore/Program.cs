using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using ValidateCopiedFileLengthCore;

namespace FetchS3FileInfoCore
{
    public class Program
    {
        static string accessKeyId = ConfigurationManager.AppSettings["accessKeyId"];
        static string secretAccessKey = ConfigurationManager.AppSettings["secretAccessKey"];
        static string bucketName = ConfigurationManager.AppSettings["bucketName"];
        static string path = ConfigurationManager.AppSettings["path"];
        private static string specifiedContinuationTokenStr = ConfigurationManager.AppSettings["continuationToken"];

        private const string OutputPath = Constants.FetchS3FileInfo + "/" + Constants.OutputPath;
        private const string LogPath = Constants.FetchS3FileInfo + "/" + Constants.LogPath;

        static string continuationToken = specifiedContinuationTokenStr;

        static AmazonS3Config config = new AmazonS3Config()
        {
            Timeout = TimeSpan.FromSeconds(300),
            ReadWriteTimeout = TimeSpan.FromSeconds(600),
            RegionEndpoint = RegionEndpoint.USEast1
        };
        static BasicAWSCredentials credential = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        static AmazonS3Client s3Client = new AmazonS3Client(credential, config);
        static string logFilePath = string.Empty;
        static string outputFilePath = string.Empty;
        static long fileCount = 0;

        static void Main(string[] args)
        {
            var now = DateTime.Now.ToString("yyyy_MM_dd HH_mm_ss");
            outputFilePath = Helper.PathCombine(OutputPath, "Output_" + now + ".txt");
            logFilePath = Helper.PathCombine(LogPath, "Log_" + now + ".txt");

            if (!string.IsNullOrWhiteSpace(path))
            {
                if (!path.EndsWith("/"))
                {
                    path += "/";
                }
            }

            try
            {
                Helper.InitDirectory(OutputPath);
                Helper.InitDirectory(LogPath);

                FileCountAsync().Wait();
               
                File.AppendAllLines(logFilePath,
                    new string[] { "Fetch S3 file info completed. Total file count: " + fileCount });
                Console.WriteLine("Fetch S3 file info completed. Total file count: " + fileCount); 
                
            }
            catch (Exception e)
            {
                File.AppendAllLines(logFilePath,
                    new string[] { "Fetch S3 file info failed: " + Helper.GetLogString(e, true) });
                Console.WriteLine("Fetch S3 file info failed: " + Helper.GetLogString(e, true));
                throw;
            }
        }

        static async Task FileCountAsync()
        {
            s3Client = await SwitchClient(s3Client);

            fileCount = 0;
            do
            {
                ListObjectsRequest req = new ListObjectsRequest()
                {
                    BucketName = bucketName,
                    Prefix = path,
                    Marker = continuationToken
                };
                var resp = s3Client.ListObjectsAsync(req).ConfigureAwait(false).GetAwaiter().GetResult();

                continuationToken = resp.IsTruncated ? resp.NextMarker : null;

                var fileItemInfos = ToFileItemInfos(resp.S3Objects, Helper.PathCombine(bucketName, path))
                    .ToList();
                fileCount += fileItemInfos.Count;

                File.AppendAllLines(outputFilePath, fileItemInfos.ToLines());
                File.AppendAllLines(logFilePath,
                    new string[]
                    {
                        string.Format(CultureInfo.InvariantCulture,
                            "Fetched S3 file count: {0}. Continuation token: '{1}'.", fileCount,
                            continuationToken)
                    });
                Console.WriteLine("Fetched S3 file count: " + fileCount);

            } while (!string.IsNullOrWhiteSpace(continuationToken));
        }

        private static IEnumerable<FileItemInfo> ToFileItemInfos(IEnumerable<S3Object> s3Objects, string basePath)
        {
            foreach (var s3Object in s3Objects)
            {
                if (!s3Object.Key.EndsWith("/"))
                {
                    var element = new FileItemInfo()
                    {
                        Name = Helper.RemoveBasePath(Helper.PathCombine(s3Object.BucketName, s3Object.Key), basePath),
                        Length = s3Object.Size,
                        LastModifiedUtc = s3Object.LastModified.ToUniversalTime()
                    };
                    yield return element;
                }
            }
        }

        private static async Task<AmazonS3Client> SwitchClient(AmazonS3Client s3Client)
        {
            RegionEndpoint regionEp = null;
            string uniqueKey =
                string.Format(CultureInfo.InvariantCulture, "{0}_{1}", accessKeyId,
                    bucketName); // GetUniqueNameFromAccessKeyIdAndBucketName

            
            GetBucketLocationResponse resp = await s3Client.GetBucketLocationAsync(bucketName);

            if (resp == null)
            {
                return s3Client;
            }
            else
            {
                regionEp = RegionToEndpoint(resp.Location);

            }

            if (regionEp != null && !regionEp.Equals(s3Client.Config.RegionEndpoint))
            {
                s3Client.Dispose();
                AmazonS3Config config = new AmazonS3Config()
                {
                    Timeout = TimeSpan.FromSeconds(300),
                    ReadWriteTimeout = TimeSpan.FromSeconds(600),
                    RegionEndpoint = regionEp
                };
                var credential = new BasicAWSCredentials(accessKeyId, secretAccessKey);
                s3Client = new AmazonS3Client(credential, config);
            }

            return s3Client;
        }

        internal static RegionEndpoint RegionToEndpoint(S3Region region)
        {
            RegionEndpoint regionEp;
            if (String.IsNullOrEmpty(region.Value))//By AWS doc, if empty, it is USEast1.
            {
                regionEp = RegionEndpoint.USEast1;
            }
            else
            {
                if (!RegionAliasTable.TryGetValue(region.Value, out regionEp))
                {
                    regionEp = RegionEndpoint.GetBySystemName(region.Value);
                }
            }
            return regionEp;
        }

        //Per http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETlocation.html
        //AWS may return region alias, but this only happen on 'eu-west-1' which alias is 'EU'. 
        private static readonly IDictionary<string, RegionEndpoint> RegionAliasTable =
            new Dictionary<string, RegionEndpoint>()
            {
                { "EU", RegionEndpoint.EUWest1 }
            };
    }
}

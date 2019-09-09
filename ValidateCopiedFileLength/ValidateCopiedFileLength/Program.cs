using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ValidateCopiedFileLength
{
    class Program
    {
        static string adhocValidateSourceFolderPath = ConfigurationManager.AppSettings["adhocValidateSourceFolderPath"];
        static string adhocValidateTargetFolderPath = ConfigurationManager.AppSettings["adhocValidateTargetFolderPath"];

        private static string logFilePath = "";

        private const string OutputPath = Constants.ValidateCopiedFileLength + "/" + Constants.OutputPath;
        private const string LogPath = Constants.ValidateCopiedFileLength + "/" + Constants.LogPath;

        static void Main(string[] args)
        {
            var now = DateTime.Now.ToString("yyyy_MM_dd HH_mm_ss");
            var outputFilePath = Helper.PathCombine(OutputPath, "Output_" + now + ".txt");
            logFilePath = Helper.PathCombine(LogPath, "Log_" + now + ".txt");

            try
            {
                Helper.InitDirectory(OutputPath);
                Helper.InitDirectory(LogPath);

                var sourceFileLengthInfo = new Dictionary<string, FileItemInfo>();
                var targetFileLengthInfo = new Dictionary<string, FileItemInfo>();

                if (string.IsNullOrWhiteSpace(adhocValidateSourceFolderPath) ||
                    string.IsNullOrWhiteSpace(adhocValidateTargetFolderPath))
                {
                    LoadFromLatestFile(Constants.FetchS3FileInfo + "/" + Constants.OutputPath,
                        sourceFileLengthInfo);
                    LoadFromLatestFile(Constants.FetchBlobFileInfo + "/" + Constants.OutputPath,
                        targetFileLengthInfo);
                }
                else
                {
                    LoadFromAllFiles(adhocValidateSourceFolderPath, sourceFileLengthInfo);
                    LoadFromAllFiles(adhocValidateTargetFolderPath, targetFileLengthInfo);
                }

                Log("Source file count: " + sourceFileLengthInfo.Count);
                Log("Target file count: " + targetFileLengthInfo.Count);
                int validatedFileCount = 0;
                int notFoundFieCountInTarget = 0;
                int lengthDifferentForSourceChangedFileCount = 0;
                int count = 0;
                List<string> validateFailedFiles = new List<string>();
                foreach (var fileName in sourceFileLengthInfo.Keys)
                {
                    if (targetFileLengthInfo.ContainsKey(fileName))
                    {
                        if (sourceFileLengthInfo[fileName].Length != targetFileLengthInfo[fileName].Length)
                        {
                            if (sourceFileLengthInfo[fileName].LastModifiedUtc <
                                targetFileLengthInfo[fileName].LastModifiedUtc)
                            {
                                validateFailedFiles.Add(string.Join(", ", fileName,
                                    sourceFileLengthInfo[fileName].Length,
                                    sourceFileLengthInfo[fileName].LastModifiedUtc,
                                    targetFileLengthInfo[fileName].Length,
                                    targetFileLengthInfo[fileName].LastModifiedUtc));

                                string message = string.Format(CultureInfo.InvariantCulture,
                                    "Validate failed. File name: '{0}'. Source length: {1}, last modified: {2}. Target length: {3}, last modified: {4}.",
                                    fileName, sourceFileLengthInfo[fileName].Length,
                                    sourceFileLengthInfo[fileName].LastModifiedUtc,
                                    targetFileLengthInfo[fileName].Length,
                                    targetFileLengthInfo[fileName].LastModifiedUtc);
                                Log(message);
                            }
                            else
                            {
                                lengthDifferentForSourceChangedFileCount++;

                                string message = string.Format(CultureInfo.InvariantCulture,
                                    "Found file length difference caused by source file changed. File name: '{0}'. Source length: {1}, last modified: {2}. Target length: {3}, last modified: {4}.",
                                    fileName, sourceFileLengthInfo[fileName].Length,
                                    sourceFileLengthInfo[fileName].LastModifiedUtc,
                                    targetFileLengthInfo[fileName].Length,
                                    targetFileLengthInfo[fileName].LastModifiedUtc);
                                Log(message);
                            }
                        }

                        validatedFileCount++;
                    }
                    else
                    {
                        notFoundFieCountInTarget++;
                    }

                    if (++count % 100000 == 0)
                    {
                        Log(string.Format(CultureInfo.InvariantCulture, "{0} files in source are checked: \r\n  {1} files are validated based on target: {2} files met the length mismatch; \r\n  {3} files not found in target.",
                            count, validatedFileCount, validateFailedFiles.Count, notFoundFieCountInTarget));
                    }
                }

                File.AppendAllLines(outputFilePath, validateFailedFiles);
                Log(string.Format(CultureInfo.InvariantCulture, "Validate copied file length finished. {0} files in source are checked: \r\n  {1} files are validated based on target: {2} files met the length mismatch; \r\n  {3} files not found in target.",
                    count, validatedFileCount, validateFailedFiles.Count, notFoundFieCountInTarget));
            }
            catch (Exception e)
            {
                Log("Validate copied file length failed: " + Helper.GetLogString(e, true));
                throw;
            }
        }

        private static void LoadFromLatestFile(string directoryPath, IDictionary<string, FileItemInfo> info)
        {
            var di = new DirectoryInfo(directoryPath);
            if (!di.Exists)
            {
                Log("Folder doesn't exist: " + di.FullName);
            }

            var latestFi = di.GetFiles("*", SearchOption.AllDirectories).OrderByDescending(f => f.LastWriteTime)
                .FirstOrDefault();

            if (latestFi != null)
            {
                LoadFileLengthInfo(latestFi.FullName, info);
            }
            else
            {
                Log("No file under folder: " + di.FullName);
            }
        }

        private static void LoadFromAllFiles(string directoryPath, IDictionary<string, FileItemInfo> info)
        {
            var di = new DirectoryInfo(directoryPath);
            if (!di.Exists)
            {
                Log("Folder doesn't exist: " + di.FullName);
            }

            var fis = di.GetFiles("*", SearchOption.AllDirectories);

            if (fis != null)
            {
                foreach (var fi in fis)
                {
                    LoadFileLengthInfo(fi.FullName, info);
                }
            }
            else
            {
                Log("No file under folder: " + di.FullName);
            }
        }

        private static void LoadFileLengthInfo(string path, IDictionary<string, FileItemInfo> info)
        {
            if (info == null)
            {
                throw new ArgumentNullException(nameof(info));
            }

            int count = 0;
            foreach (var line in File.ReadLines(path))
            {
                var splits = line.Split(',');
                info[splits[0]] = new FileItemInfo()
                {
                    Length = long.Parse(splits[1]),
                    LastModifiedUtc = DateTime.Parse(splits[2])
                };

                if (++count % 100000 == 0)
                {
                    Log(string.Format(CultureInfo.InvariantCulture, "Load file info from path finished: '{0}'. Loaded file count: {1}", path, info.Count));
                }
            }

            Log(string.Format(CultureInfo.InvariantCulture, "Load file info from path finished: '{0}'. Loaded file count: {1}", path, info.Count));
        }

        private static void Log(string message)
        {
            File.AppendAllLines(logFilePath, new string[] { message });
            Console.WriteLine(message);
        }
    }
}

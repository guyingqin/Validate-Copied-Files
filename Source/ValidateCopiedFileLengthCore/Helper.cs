using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ValidateCopiedFileLengthCore
{
    public static class Helper
    {
        public static void InitDirectory(string path)
        {
            DirectoryInfo di = new DirectoryInfo(path);
            if (!di.Exists)
            {
                di.Create();
            }
        }

        public static string PathCombine(string patha, string pathb, char separator = '/')
        {
            if (string.IsNullOrEmpty(patha))
            {
                return pathb;
            }
            if (string.IsNullOrEmpty(pathb))
            {
                return patha;
            }

            return patha.TrimEnd(separator) + separator + pathb.TrimStart(separator);
        }

        public static string RemoveBasePath(string fullPath, string basePath, char separator = '/')
        {
            if (string.IsNullOrEmpty(basePath) || string.IsNullOrEmpty(fullPath))
            {
                return (fullPath ?? string.Empty).TrimStart(separator);
            }
            // Note that itemPath is relative path so always doesn't start with '/'
            fullPath = fullPath.TrimStart(separator);
            basePath = basePath.TrimStart(separator);
            if (fullPath.StartsWith(basePath, StringComparison.OrdinalIgnoreCase))
            {
                fullPath = fullPath.Substring(basePath.Length);
            }
            return fullPath.TrimStart(separator);
        }

        public const int LogStringLengthCap = 1024 * 1024;
        public static string GetLogString(Exception exception, bool includeStackTrace, Func<Exception, String> exceptionMsgExtractor)
        {
            if (null == exception)
            {
                throw new ArgumentNullException("exception", "Argument: exception is null.");
            }

            StringBuilder logString = new StringBuilder();

            IEnumerable<Exception> exceptionList = FlattenException(exception);

            try
            {
                foreach (Exception innerException in exceptionList)
                {
                    if (logString.Length > LogStringLengthCap)
                    {
                        break;
                    }

                    logString.Append("'");
                    logString.AppendFormat("{0}={1},", "Type", innerException.GetType().FullName);
                    logString.AppendFormat("{0}={1},", "Message", null == innerException.Message ? string.Empty : innerException.Message);
                    logString.AppendFormat("{0}={1},", "Source", null == innerException.Source ? string.Empty : innerException.Source);
                    
                    if (exceptionMsgExtractor != null)
                    {
                        String message = exceptionMsgExtractor(innerException);
                        if (!String.IsNullOrEmpty(message))
                        {
                            logString.Append(message).Append(",");
                        }
                    }

                    if (includeStackTrace && innerException.StackTrace != null)
                    {
                        logString.AppendFormat("{0}={1},", "StackTrace", innerException.StackTrace);
                    }

                    logString.Append("'");
                }
            }
            catch (OutOfMemoryException)
            {
            }

            return logString.ToString();
        }

        public static string GetLogString(Exception exception, bool includeStackTrace)
        {
            return GetLogString(exception, includeStackTrace, ExtractMsg);
        }

        private static String ExtractMsg(Exception exception)
        {
            return null;
        }

        public static IEnumerable<Exception> FlattenException(Exception exception)
        {
            if (exception.InnerException == null)
            {
                yield return exception;
            }
            else
            {
                AggregateException aggregateException = exception as AggregateException;
                if (aggregateException != null)
                {
                    foreach (Exception ex in aggregateException.InnerExceptions)
                    {
                        // recursively append child exceptions
                        IEnumerable<Exception> childCollection = FlattenException(ex);
                        foreach (Exception child in childCollection)
                        {
                            yield return child;
                        }
                    }
                }
                else
                {
                    yield return exception;

                    // call this function recursively, since it can happen that the child is an AggregateException
                    IEnumerable<Exception> childCollection = FlattenException(exception.InnerException);
                    foreach (Exception child in childCollection)
                    {
                        yield return child;
                    }
                }
            }
        }



        public static IEnumerable<string> ToLines(this IEnumerable<FileItemInfo> infos)
        {
            foreach (var info in infos)
            {
                yield return info.ToString();
            }
        }
    }
}

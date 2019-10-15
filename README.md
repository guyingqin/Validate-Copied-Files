# Inventory audit tool (.NET Core based) for AWS S3 and Azure Blob store
This tool is made up of 3 utilities which can be used seperately or together and can be useful to audit the files copied from AWS S3 to Azure Storage (Blob) and also returns the list of files which are not copied successfully from AWS S3 to Azure Storage and the log about what may be wrong with those files, in case of missing files one can always look at the log of both utilities and figure out which files may be missing. Based on .NET Core works for both Windows and Linux.

### Repository strcuture
  - /Source: includes the source codes.
    - FetchBlobFileInfoCore - .net core (2.2) based app to download list of objects in a given Azure Storage Container.
    - FetchS3FileInfoCore - .net core (2.2) based app to download list of objects in a given AWS S3 bucket
    - ValidateCopiedFileLengthCore - Looks at the log of both above mentioned utilities, gives a count and also logs information about files missing in destination Azure storage container.
  - /Tool: includes the executable files for both linux and Windows platforms and shell/batch scripts to run the tool.

### .NET Core Installation
Follow steps from here to install .NET Core on your machine https://dotnet.microsoft.com/download. For running the tool you may install the runtime only and for compiling the code opt for SDK installation. The tool is compiled to run on .NET Core 2.2.
 

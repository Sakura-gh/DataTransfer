using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace DataTransfer
{
    public static class CloudBlobUtil
    {
        public static CloudBlockBlob getBlob(string name)
        {
            string storageConnectionString = System.Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference("shdprod");

            CloudBlockBlob inputBlob = container.GetBlockBlobReference(name);

            return inputBlob;
        }

        public static CloudBlockBlob getInputBlob()
        {
            string inputBlobPath = System.Environment.GetEnvironmentVariable("InputBlobPath");
            CloudBlockBlob inputBlob = getBlob(inputBlobPath);

            return inputBlob;
        }

        public static CloudBlockBlob getOutputBlob()
        {
            string outputBlobPath = System.Environment.GetEnvironmentVariable("OutputBlobPath");
            CloudBlockBlob outputBlob = getBlob(outputBlobPath);

            return outputBlob;
        }
    }
}

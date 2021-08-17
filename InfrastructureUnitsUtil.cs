using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace DataTransfer
{
    public static class InfrastructureUnitsUtil
    {
        public static async Task AsnTopology()
        {
            // asn path
            string inputPath = "...";
            
            // Get the AAD credential.
            AuthenticationHeaderValue bearerToken = await GetAADCredential();

            // Get the input blob configuration.
            CloudBlockBlob inputBlob = GetBlob(inputPath);

            using (var memoryStream = new MemoryStream())
            {
                await inputBlob.DownloadToStreamAsync(memoryStream);
                string stream2string = Encoding.ASCII.GetString(memoryStream.ToArray());
                var asnArray = stream2string.Split("\n");

                Guid guid = Guid.NewGuid();

                List<InfrastructureUnitDefinitions> infrastructureUnitDefinitions = new List<InfrastructureUnitDefinitions>();

                infrastructureUnitDefinitions.Add(
                    new InfrastructureUnitDefinitions
                    {
                        DisplayName = "ASN",
                        Entity = "ASN",
                        IsDeleted = false,
                        Name = "ASN",
                        ParentEntity = "Region",
                        ParentName = "WW"
                    });

                foreach (var asn in asnArray)
                {
                    if (!string.IsNullOrEmpty(asn))
                    {
                        //var obj = JsonConvert.DeserializeObject<Asn>(asn);
                        infrastructureUnitDefinitions.Add(
                            new InfrastructureUnitDefinitions
                            {
                                DisplayName = asn,
                                Entity = asn,
                                IsDeleted = false,
                                Name = asn,
                                ParentEntity = "ASN",
                                ParentName = "ASN"
                            });
                    }
                }

                // Set Infrastructure Units.
                await PostForTopology(guid, infrastructureUnitDefinitions, bearerToken);
                await CommitForTopology(guid, bearerToken);
            }
        }

        public static async Task TenantMapping(List<TenantAsn> tenantAsnList)
        {
            // asn path
            string inputPath = "...";

            // get the AAD credential.
            AuthenticationHeaderValue bearerToken = await GetAADCredential();

            // get the guid
            Guid guid = Guid.NewGuid();

            List<TenantInfrastructureUnit> tenantInfrastructureUnits = new List<TenantInfrastructureUnit>();
            int request = 0;
            foreach (TenantAsn tenantAsn in tenantAsnList)
            {
                tenantInfrastructureUnits.Add(
                    new TenantInfrastructureUnit
                    {
                        EntityId = new Guid(tenantAsn.tenantId),
                        IsDeleted = false,
                        InfrastructureUnitName = tenantAsn.asn,
                        Weight = 0
                    });

                if (++request > 2000)
                {
                    await PostForTenant(guid, tenantInfrastructureUnits, bearerToken);
                    tenantInfrastructureUnits.Clear();
                    request = 0;
                }
            }

            await PostForTenant(guid, tenantInfrastructureUnits, bearerToken);
            await CommitForTenant(guid, bearerToken);
        }

        public static CloudBlockBlob GetBlob(string path)
        {
            string storageConnectionString = System.Environment.GetEnvironmentVariable("STORAGE_CONNECTION_URL");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference("shdprod");
            CloudBlockBlob blob = container.GetBlockBlobReference(path);

            return blob;
        }

        public static async Task<string> PostForTopology(
            Guid guid, 
            List<InfrastructureUnitDefinitions> infrastructureUnitDefinitions, 
            AuthenticationHeaderValue bearerToken)
        {
            string postUri = "https://eventauthoringppe.trafficmanager.net/v1.0/Workloads({0})/SetInfrastructureUnits";
            var postContents = new StringContent(
                JsonConvert.SerializeObject(
                    new Dictionary<string, object>
                    {
                        { "transactionId", guid },
                        {
                            "infrastructureUnitDefinitions",
                            infrastructureUnitDefinitions
                        }
                    }));
            postContents.Headers.ContentType = new MediaTypeHeaderValue("application/json");
           
            var httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Authorization = bearerToken;
            var response = await httpClient.PostAsync(postUri, postContents);

            return response.ToString();
        }

        public static async Task<string> CommitForTopology(
            Guid guid,
            AuthenticationHeaderValue bearerToken)
        {
            string commitUri = "https://eventauthoringppe.trafficmanager.net/v1.0/Workloads({0})/CommitInfrastructureUnits";
            var commitContents = new StringContent(
                JsonConvert.SerializeObject(
                    new Dictionary<string, object>
                    {
                        { "transactionId", guid },
                    }));
            commitContents.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            
            var httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Authorization = bearerToken;
            var response = await httpClient.PostAsync(commitUri, commitContents);

            return response.ToString();
        }

        public static async Task<string> PostForTenant(
            Guid guid,
            List<TenantInfrastructureUnit> tenantInfrastructureUnits,
            AuthenticationHeaderValue bearerToken)
        {
            string postUri = "https://eventauthoringppe.trafficmanager.net/v1.0/Workloads({0})/SetTenantInfrastructureUnits";
            var postContents = new StringContent(
                JsonConvert.SerializeObject(
                    new Dictionary<string, object>
                    {
                        { "transactionId", guid },
                        {
                            "tenantInfrastructureUnits",
                            tenantInfrastructureUnits
                        }
                    }));
            postContents.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            var httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Authorization = bearerToken;
            var response = await httpClient.PostAsync(postUri, postContents);

            return response.ToString();
        }

        public static async Task<string> CommitForTenant(
            Guid guid,
            AuthenticationHeaderValue bearerToken)
        {
            string commitUri = "https://eventauthoringppe.trafficmanager.net/v1.0/Workloads({0})/CommitTenantInfrastructureUnits";
            var commitContents = new StringContent(
                JsonConvert.SerializeObject(
                    new Dictionary<string, object>
                    {
                        { "transactionId", guid },
                    }));
            commitContents.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            var httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Authorization = bearerToken;
            var response = await httpClient.PostAsync(commitUri, commitContents);

            return response.ToString();
        }

        public static async Task<AuthenticationHeaderValue> GetAADCredential()
        {
            // AAD parameters.
            string aadInstance = "https://login.windows.net/{0}";
            string serviceResourceId = "https://eventauthoring.cloudapp.net";
            string tenant = System.Environment.GetEnvironmentVariable("AADTenantId");
            string clientId = System.Environment.GetEnvironmentVariable("AADClientId");
            string appKey = System.Environment.GetEnvironmentVariable("AADAppKey"); ;

            // Get auth token and add the access token to the authorization header of the request.
            var authContext = new AuthenticationContext(string.Format(CultureInfo.InvariantCulture, aadInstance, tenant));
            var clientCredential = new ClientCredential(clientId, appKey);
            AuthenticationResult authResult = await authContext.AcquireTokenAsync(serviceResourceId, clientCredential);
            var token = new AuthenticationHeaderValue("Bearer", authResult.AccessToken);

            return token;
        }
    }

    public class Asn
    {
        public string ASN { get; set; }
    }

    public class InfrastructureUnitDefinitions
    {
        /// <summary>
        /// Gets or sets the display name.
        /// </summary>
        public string DisplayName { get; set; }

        /// <summary>
        /// Gets or sets the entity.    
        /// </summary>
        public string Entity { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether is deleted.
        /// </summary>
        public bool IsDeleted { get; set; }

        /// <summary>
        /// Gets or sets the name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the parent entity.
        /// </summary>
        public string ParentEntity { get; set; }

        /// <summary>
        /// Gets or sets the parent name.
        /// </summary>
        public string ParentName { get; set; }
    }

    /// <summary>
    /// The tenant infrastructure unit.
    /// </summary>
    public class TenantInfrastructureUnit
    {
        /// <summary>
        /// Gets or sets the entity id.
        /// </summary>
        public Guid EntityId { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether is deleted.
        /// </summary>
        public bool IsDeleted { get; set; }

        /// <summary>
        /// Gets or sets the infrastructure unit name.
        /// </summary>
        public string InfrastructureUnitName { get; set; }

        /// <summary>
        /// Gets or sets the weight.
        /// </summary>
        public int? Weight { get; set; }
    }
}

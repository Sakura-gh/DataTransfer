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
        public static async Task EFZTopology()
        {
            try
            {
                // asn path
                string inputPath = "Datasets/RegionTopology.json";

                // Get the AAD credential.
                AuthenticationHeaderValue bearerToken = GetAADCredential();

                // Get the input blob configuration.
                CloudBlockBlob inputBlob = CloudBlobUtil.getBlob(inputPath);

                using (var memoryStream = new MemoryStream())
                {
                    await inputBlob.DownloadToStreamAsync(memoryStream);
                    string stream2string = Encoding.ASCII.GetString(memoryStream.ToArray());
                    var regionArray = stream2string.Split("\n");

                    Guid guid = Guid.NewGuid();
                    StringBuilder sb = new StringBuilder();
                    List<InfrastructureUnitDefinitions> infrastructureUnitDefinitions = new List<InfrastructureUnitDefinitions>();

                    infrastructureUnitDefinitions.Add(
                        new InfrastructureUnitDefinitions
                        {
                            DisplayName = "EFZ",
                            Entity = "EFZ",
                            IsDeleted = false,
                            Name = "EFZ",
                            ParentEntity = "Region",
                            ParentName = "WW"
                        });

                    foreach (var item in regionArray)
                    {
                        if (!string.IsNullOrEmpty(item))
                        {
                            string item0 = item.Replace("?", "");
                            var obj = JsonConvert.DeserializeObject<Region>(item0.Replace(".", ""));
                            string regionName = obj.EFZ;

                            if (regionName.Contains("'"))
                            {
                                regionName = regionName.Replace("\'", "\'\'");
                            }

                            infrastructureUnitDefinitions.Add(
                                new InfrastructureUnitDefinitions
                                {
                                    DisplayName = regionName,
                                    Entity = regionName,
                                    IsDeleted = false,
                                    Name = regionName,
                                    ParentEntity = "EFZ",
                                    ParentName = "EFZ"
                                });
                        }
                    }
                    // full refresh
                    await SetForTopology(guid, infrastructureUnitDefinitions, bearerToken);
                    await CommitForTopology(guid, bearerToken);

                    // incremental
                    //string response = await ModifyForTopology(infrastructureUnitDefinitions, bearerToken);
                }
            } catch (Exception e)
            {
                TimeLogger.Log("write topology error: " + e.Message);
            }
        }

        public static async Task EFZTenantMapping()
        {
            try
            {
                string inputPath = "TenantMapping/Latest/TenantMapping.json";

                // get the AAD credential.
                AuthenticationHeaderValue bearerToken = GetAADCredential();

                // Get the input blob configuration.
                CloudBlockBlob inputBlob = CloudBlobUtil.getBlob(inputPath);

                using (var memoryStream = new MemoryStream())
                {
                    await inputBlob.DownloadToStreamAsync(memoryStream);
                    string stream2string = Encoding.ASCII.GetString(memoryStream.ToArray());
                    var lineArray = stream2string.Split("\n");

                    Guid guid = Guid.NewGuid();
                    StringBuilder sb = new StringBuilder();
                    List<TenantInfrastructureUnit> tenantInfrastructureUnits = new List<TenantInfrastructureUnit>();

                    int request = 0;
                    HashSet<string> hashSet = new HashSet<string>();

                    foreach (var item in lineArray)
                    {

                        if (!string.IsNullOrEmpty(item))
                        {
                            string item0 = item.Replace("?", "");
                            var obj = JsonConvert.DeserializeObject<TenantEFZ>(item0.Replace(".", ""));
                            string tenantId = obj.OMSTenantId;
                            string efz = obj.source_EFZ;

                            hashSet.Add(tenantId + "@" + efz);
                        }
                    }

                    foreach (string line in hashSet)
                    {
                        string[] splitArray = line.Split("@");

                        if (splitArray.Length == 2)
                        {
                            string regionSplit = splitArray[1];
                            string tenantSplit = splitArray[0];

                            tenantInfrastructureUnits.Add(
                                new TenantInfrastructureUnit
                                {
                                    EntityId = new Guid(tenantSplit),
                                    IsDeleted = false,
                                    InfrastructureUnitName = regionSplit,
                                    Weight = 0
                                });
                        }

                        request++;

                        if (++request > 2000)
                        {
                            await SetForTenant(guid, tenantInfrastructureUnits, bearerToken);
                            tenantInfrastructureUnits.Clear();
                            request = 0;
                        }
                    }

                    // full refresh
                    await SetForTenant(guid, tenantInfrastructureUnits, bearerToken);
                    await CommitForTenant(guid, bearerToken);

                    // refresh
                    //string response = await ModifyForTenant(tenantInfrastructureUnits, bearerToken);
                }
            } catch (Exception e)
            {
                TimeLogger.Log("write tenant error: " + e.Message);
            }
        }

        public static async Task AsnTopology()
        {
            try
            {
                // asn path
                string inputPath = "TenantMapping/EXO_Top200ASNs.csv";

                // Get the AAD credential.
                AuthenticationHeaderValue bearerToken = GetAADCredential();

                // Get the input blob configuration.
                CloudBlockBlob inputBlob = CloudBlobUtil.getBlob(inputPath);

                using (var memoryStream = new MemoryStream())
                {
                    await inputBlob.DownloadToStreamAsync(memoryStream);
                    string stream2string = Encoding.ASCII.GetString(memoryStream.ToArray());
                    var asnArray = stream2string.Split("\n");

                    //Guid guid = Guid.NewGuid();

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
                        string _asn = asn.Trim('?').Trim();
                        if (!string.IsNullOrEmpty(_asn))
                        {
                            //var obj = JsonConvert.DeserializeObject<Asn>(asn);
                            infrastructureUnitDefinitions.Add(
                                new InfrastructureUnitDefinitions
                                {
                                    DisplayName = _asn,
                                    Entity = _asn,
                                    IsDeleted = false,
                                    Name = _asn,
                                    ParentEntity = "ASN",
                                    ParentName = "ASN"
                                });
                        }
                    }

                    //// full refresh Infrastructure Units.
                    //await SetForTopology(guid, infrastructureUnitDefinitions, bearerToken);
                    //await CommitForTopology(guid, bearerToken);

                    // incremental
                    await ModifyForTopology(infrastructureUnitDefinitions, bearerToken);
                }
            } catch (Exception e)
            {
                TimeLogger.Log("write topology error: " + e.Message);
            }
        }

        public static async Task AsnTenantMapping(List<TenantAsn> tenantAsnList, Guid? guid = null)
        {
            try
            {
                // get the AAD credential.
                AuthenticationHeaderValue bearerToken = GetAADCredential();

                //// get the guid
                //Guid guid = Guid.NewGuid();

                List<TenantInfrastructureUnit> tenantInfrastructureUnits = new List<TenantInfrastructureUnit>();
                int request = 0;
                foreach (TenantAsn tenantAsn in tenantAsnList)
                {
                    if (string.IsNullOrEmpty(tenantAsn.asn))
                    {
                        continue;
                    }
                    tenantInfrastructureUnits.Add(
                        new TenantInfrastructureUnit
                        {
                            EntityId = new Guid(tenantAsn.tenantId),
                            IsDeleted = false,
                            InfrastructureUnitName = tenantAsn.asn,
                            Weight = 0
                        });

                    //if (++request > 2000)
                    //{
                    //    await SetForTenant((Guid)guid, tenantInfrastructureUnits, bearerToken);
                    //    tenantInfrastructureUnits.Clear();
                    //    request = 0;
                    //}
                }

                // full refresh
                //await SetForTenant((Guid)guid, tenantInfrastructureUnits, bearerToken);
                //await CommitForTenant(guid, bearerToken);

                // incremental
                await ModifyForTenant(tenantInfrastructureUnits, bearerToken);
            } catch (Exception e)
            {
                TimeLogger.Log("write tenant error: " + e.Message);
            }
        }

        public static async Task AsnTenantMappingFinished(Guid guid)
        {
            // get the AAD credential.
            AuthenticationHeaderValue bearerToken = GetAADCredential();
            // commit
            await CommitForTenant(guid, bearerToken);
        }

        public static async Task<string> SetForTopology(
            Guid guid, 
            List<InfrastructureUnitDefinitions> infrastructureUnitDefinitions, 
            AuthenticationHeaderValue bearerToken)
        {
            string setUri = "https://eventauthoringppe.trafficmanager.net/v1.0/Workloads(191)/SetInfrastructureUnits";
            var setContents = new StringContent(
                JsonConvert.SerializeObject(
                    new Dictionary<string, object>
                    {
                        { "transactionId", guid },
                        {
                            "infrastructureUnitDefinitions",
                            infrastructureUnitDefinitions
                        }
                    }));
            setContents.Headers.ContentType = new MediaTypeHeaderValue("application/json");
           
            var httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Authorization = bearerToken;
            var response = await httpClient.PostAsync(setUri, setContents);
            var resBody = response.Content.ReadAsStringAsync().Result;

            return resBody.ToString();
        }

        public static async Task<string> CommitForTopology(
            Guid guid,
            AuthenticationHeaderValue bearerToken)
        {
            string commitUri = "https://eventauthoringppe.trafficmanager.net/v1.0/Workloads(191)/CommitInfrastructureUnits";
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
            var resBody = response.Content.ReadAsStringAsync().Result;

            return resBody.ToString();
        }

        public static async Task<string> ModifyForTopology(
            List<InfrastructureUnitDefinitions> infrastructureUnitDefinitions,
            AuthenticationHeaderValue bearerToken)
        {
            string modifyUri = "https://eventauthoringppe.trafficmanager.net/v1.0/Workloads(191)/ModifyInfrastructureUnits";
            var modifyContents = new StringContent(
                JsonConvert.SerializeObject(infrastructureUnitDefinitions));
            modifyContents.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            var httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Authorization = bearerToken;
            var response = await httpClient.PostAsync(modifyUri, modifyContents);
            var resBody = response.Content.ReadAsStringAsync().Result;

            return resBody.ToString();
        }

        public static async Task<string> SetForTenant(
            Guid guid,
            List<TenantInfrastructureUnit> tenantInfrastructureUnits,
            AuthenticationHeaderValue bearerToken)
        {
            string postUri = "https://eventauthoringppe.trafficmanager.net/v1.0/Workloads(191)/SetTenantInfrastructureUnits";
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
            var resBody = response.Content.ReadAsStringAsync().Result;

            return resBody.ToString();
        }

        public static async Task<string> CommitForTenant(
            Guid guid,
            AuthenticationHeaderValue bearerToken)
        {
            string commitUri = "https://eventauthoringppe.trafficmanager.net/v1.0/Workloads(191)/CommitTenantInfrastructureUnits";
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
            var resBody = response.Content.ReadAsStringAsync().Result;

            return resBody.ToString();
        }

        public static async Task<string> ModifyForTenant(
            List<TenantInfrastructureUnit> tenantInfrastructureUnits,
            AuthenticationHeaderValue bearerToken)
        {
            string modifyUri = "https://eventauthoringppe.trafficmanager.net/v1.0/Workloads(191)/ModifyTenantInfrastructureUnits";
            var modifyContents = new StringContent(
                JsonConvert.SerializeObject(tenantInfrastructureUnits));
            modifyContents.Headers.ContentType = new MediaTypeHeaderValue("application/json");

            var httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Authorization = bearerToken;
            var response = await httpClient.PostAsync(modifyUri, modifyContents);
            var resBody = response.Content.ReadAsStringAsync().Result;

            return resBody.ToString();
        }

        public static AuthenticationHeaderValue GetAADCredential()
        {
            //// AAD parameters.
            //string aadInstance = "https://login.windows.net/{0}";
            //string serviceResourceId = "https://eventauthoring.cloudapp.net";
            //string tenant = System.Environment.GetEnvironmentVariable("AADTenantId");
            //string clientId = System.Environment.GetEnvironmentVariable("AADClientId");
            //string appKey = System.Environment.GetEnvironmentVariable("AADAppKey"); ;

            //// Get auth token and add the access token to the authorization header of the request.
            //var authContext = new AuthenticationContext(string.Format(CultureInfo.InvariantCulture, aadInstance, tenant));
            //var clientCredential = new ClientCredential(clientId, appKey);
            //AuthenticationResult authResult = await authContext.AcquireTokenAsync(serviceResourceId, clientCredential);
            //var token = new AuthenticationHeaderValue("Bearer", authResult.AccessToken);

            // Get auth token and add the access token to the authorization header of the request.
            var authContext = new AuthenticationContext(
            string.Format(
            CultureInfo.InvariantCulture,
            "https://login.windows.net/{0}",
            "cdc5aeea-15c5-4db6-b079-fcadd2505dc2"));

            var clientCredential = new ClientCredential("76ce99a7-1fb7-4ec9-9c4c-df6cd2ea1b08", "G~epTd_s2kx1e3Ug32FufiEXL91~PO1~5~");
            AuthenticationResult authResult = authContext.AcquireTokenAsync("857a8e0a-e693-45a8-bea3-2fa6bb18b63e", clientCredential).Result;
            var token = new AuthenticationHeaderValue("Bearer", authResult.AccessToken);

            return token;
        }
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

    public class Region
    {
        public string EFZ { get; set; }

    }

    public class TenantEFZ
    {
        public string OMSTenantId { get; set; }

        public string metro { get; set; }

        public string source_EFZ { get; set; }
    }
}

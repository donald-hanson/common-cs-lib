using System;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Threading.Tasks;
using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.Security.KeyVault;
using Azure.Security.KeyVault.Certificates;
using Microsoft.Extensions.Primitives;

namespace Common.Data.Services
{
    public class CertificateService
    {
        private readonly CertificateClient _certificateClient;

        public CertificateService(string vaultBaseUrl, TokenCredential tokenCredential)
        {
            _certificateClient = new CertificateClient(new Uri(vaultBaseUrl), tokenCredential);
        }

        public async Task<X509Certificate2?> GetCertificate(string certificateName)
        {
            int retries = 0;
            int maxRetries = 2;
            while (retries < maxRetries)
            {
                try
                {
                    return await InternalGetCertificate(certificateName);
                }
                catch (RequestFailedException ex)
                {
                    var statusCode = ex.Status;

                    if (statusCode == 429)
                    {
                        retries++;

                        if (retries == maxRetries)
                            throw;

                        await Task.Delay(GetWaitTime(retries));
                        continue;
                    }

                    if (statusCode == 404)
                        return null;

                    throw;
                }
            }

            return null;
        }

        public async Task SetCertificate(string certificateName, X509Certificate2 certificate)
        {
            int retries = 0;
            int maxRetries = 2;
            while (retries < maxRetries)
            {
                try
                {
                    await InternalSetCertificate(certificateName, certificate);
                    return;
                }
                catch (RequestFailedException ex)
                {
                    var statusCode = ex.Status;

                    if (statusCode == 429)
                    {
                        retries++;

                        if (retries == maxRetries)
                            throw;

                        await Task.Delay(GetWaitTime(retries));
                        continue;
                    }

                    throw;
                }
            }
        }

        private async Task<X509Certificate2?> InternalGetCertificate(string certificateName)
        {
            var response = await _certificateClient.DownloadCertificateAsync(certificateName);
            response.Value.Dispose();

            var rr = response.GetRawResponse();
            var secret = System.Text.Json.JsonSerializer.Deserialize<KeyVaultSecret>(rr.Content);
            if (secret == null)
                throw new InvalidOperationException("Could not parse certificate response");

            if (secret.ContentType == CertificateContentType.Pkcs12)
            {
                byte[] rawData = Convert.FromBase64String(secret.Value);

                return new X509Certificate2(rawData, string.Empty, X509KeyStorageFlags.Exportable);
            }

            throw new NotSupportedException($"ContentType {secret.ContentType} is not supported");

        }

        [System.Text.Json.Serialization.JsonSerializable(typeof(KeyVaultSecret))]
        internal class KeyVaultSecret
        {
            [System.Text.Json.Serialization.JsonInclude]
            [System.Text.Json.Serialization.JsonPropertyName("contentType")]
            public string ContentType { get; set; }

            [System.Text.Json.Serialization.JsonInclude]
            [System.Text.Json.Serialization.JsonPropertyName("value")]
            public string Value { get; set; }
        }

        private async Task InternalSetCertificate(string certificateName, X509Certificate2 certificate)
        {
            var certificateBytes = certificate.Export(X509ContentType.Pfx);

            var options = new ImportCertificateOptions(certificateName, certificateBytes)
            {
                Policy = new CertificatePolicy("Self", "CN=DefaultPolicy")
                {
                    Exportable = true,
                    ContentType = CertificateContentType.Pkcs12
                }
            };

            await _certificateClient.ImportCertificateAsync(options);
        }
        
        // This method implements exponential backoff if there are 429 errors from Azure Key Vault
        private static int GetWaitTime(int retryCount)
        {
            return (int)Math.Pow(2, retryCount) * 100;
        }
    }
}
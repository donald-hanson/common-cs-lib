using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Azure.Services.AppAuthentication;

namespace Common.Services;

public interface IKeyVaultService
    {
        Task<string> GetSecret(string secretName);
        Task SetSecret(string secretName, string secretValue, DateTime? expires = null);
    }

    public class KeyVaultService : IKeyVaultService
    {
        private readonly string _vaultBaseUrl;

        public KeyVaultService(string vaultBaseUrl)
        {
            _vaultBaseUrl = vaultBaseUrl;
        }

        public async Task<string> GetSecret(string secretName)
        {
            int retries = 0;
            int maxRetries = 2;
            while (retries < maxRetries)
            {
                try
                {
                    return await InternalGetSecret(secretName);
                }
                catch (KeyVaultErrorException ex)
                {
                    var statusCode = (int?)ex.Response?.StatusCode ?? 0;

                    if (statusCode == 429)
                    {
                        retries++;

                        if (retries == maxRetries)
                            throw;

                        await Task.Delay(GetWaitTime(retries));
                    }

                    if (statusCode == 404)
                        return null;

                    throw;
                }
            }

            return null;
        }

        public async Task SetSecret(string secretName, string secretValue, DateTime? expires = null)
        {
            int retries = 0;
            int maxRetries = 2;
            while (retries < maxRetries)
            {
                try
                {
                    await InternalSetSecret(secretName, secretValue, expires);
                    return;
                }
                catch (KeyVaultErrorException ex)
                {
                    var statusCode = (int?)ex.Response?.StatusCode ?? 0;

                    if (statusCode == 429)
                    {
                        retries++;

                        if (retries == maxRetries)
                            throw;

                        await Task.Delay(GetWaitTime(retries));
                    }

                    throw;
                }
            }
        }

        private async Task<string> InternalGetSecret(string secretName)
        {
            var keyVaultClient = GetKeyVaultClient();
            var vaultBaseUrl = GetVaultBaseUrl();
            var secret = await keyVaultClient.GetSecretAsync(vaultBaseUrl, secretName)
                .ConfigureAwait(false);
            
            if (secret.Attributes.Enabled == false)
            {
                return null;
            }

            if (secret.Attributes.NotBefore.HasValue && DateTime.UtcNow < secret.Attributes.NotBefore.Value)
            {
                return null;
            }

            if (secret.Attributes.Expires.HasValue && DateTime.UtcNow > secret.Attributes.Expires.Value)
            {
                return null;
            }
            
            return secret.Value;
        }

        private async Task InternalSetSecret(string secretName, string secretValue, DateTime? expires = null)
        {
            var keyVaultClient = GetKeyVaultClient();
            var vaultBaseUrl = GetVaultBaseUrl();
            SecretAttributes attributes = new SecretAttributes(true, null, expires);
            await keyVaultClient.SetSecretAsync(vaultBaseUrl, secretName, secretValue, null, null, attributes)
                .ConfigureAwait(false);
        }

        private string GetVaultBaseUrl()
        {
            return _vaultBaseUrl;
        }

        private static KeyVaultClient GetKeyVaultClient()
        {
            AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
            return new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
        }

        // This method implements exponential backoff if there are 429 errors from Azure Key Vault
        private static int GetWaitTime(int retryCount)
        {
            return (int)Math.Pow(2, retryCount) * 100;
        }
    }
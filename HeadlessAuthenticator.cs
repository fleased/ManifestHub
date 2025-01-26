using SteamKit2;
using SteamKit2.Authentication;

namespace ManifestHub;

public class HeadlessAuthenticator(AccountInfoCallback account) : IAuthenticator {
    private AccountInfoCallback _account = account;

    public Task<bool> AcceptDeviceConfirmationAsync() {
        Console.Error.WriteLine($"检测到STEAM GUARD，尝试向 {_account.AccountName} 的设备发送验证请求...");
        return Task.FromResult(true);
    }

    public Task<string> GetDeviceCodeAsync(bool previousCodeWasIncorrect) {
        return Task.FromException<string>(
            new AuthenticationException("验证失败", EResult.AccountLoginDeniedNeedTwoFactor));
    }

    public Task<string> GetEmailCodeAsync(string email, bool previousCodeWasIncorrect) {
        return Task.FromException<string>(
            new AuthenticationException("验证失败", EResult.AccountLogonDeniedVerifiedEmailRequired));
    }
}

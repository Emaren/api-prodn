export var WalletStatus;
(function (WalletStatus) {
    WalletStatus["Disconnected"] = "Disconnected";
    WalletStatus["Connecting"] = "Connecting";
    WalletStatus["Connected"] = "Connected";
    WalletStatus["NotExist"] = "NotExist";
    WalletStatus["Rejected"] = "Rejected";
    WalletStatus["Error"] = "Error";
})(WalletStatus || (WalletStatus = {}));
export const SuggestTokenTypes = {
    CW20: 'cw20',
    ERC20: 'erc20',
};

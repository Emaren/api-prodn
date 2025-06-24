"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CoreUtil = void 0;
const WALLETCONNECT_DEEPLINK_CHOICE = 'WALLETCONNECT_DEEPLINK_CHOICE';
exports.CoreUtil = {
    isHttpUrl(url) {
        return url.startsWith('http://') || url.startsWith('https://');
    },
    formatNativeUrl(appUrl, wcUri, os, name) {
        if (exports.CoreUtil.isHttpUrl(appUrl)) {
            return this.formatUniversalUrl(appUrl, wcUri, name);
        }
        const plainAppUrl = appUrl.replaceAll('/', '').replaceAll(':', '');
        exports.CoreUtil.setWalletConnectDeepLink(plainAppUrl, name);
        const encodedWcUrl = encodeURIComponent(wcUri);
        return `${plainAppUrl}://wc?uri=${encodedWcUrl}`;
    },
    formatUniversalUrl(appUrl, wcUri, name) {
        if (!exports.CoreUtil.isHttpUrl(appUrl)) {
            return this.formatNativeUrl(appUrl, wcUri, name);
        }
        let plainAppUrl = appUrl;
        if (appUrl.endsWith('/')) {
            plainAppUrl = appUrl.slice(0, -1);
        }
        exports.CoreUtil.setWalletConnectDeepLink(plainAppUrl, name);
        const encodedWcUrl = encodeURIComponent(wcUri);
        return `${plainAppUrl}/wc?uri=${encodedWcUrl}`;
    },
    async wait(miliseconds) {
        return new Promise((resolve) => {
            setTimeout(resolve, miliseconds);
        });
    },
    openHref(href, target = '_self') {
        window.open(href, target, 'noreferrer noopener');
    },
    setWalletConnectDeepLink(href, name) {
        localStorage.setItem(WALLETCONNECT_DEEPLINK_CHOICE, JSON.stringify({ href, name }));
    },
    removeWalletConnectDeepLink() {
        localStorage.removeItem(WALLETCONNECT_DEEPLINK_CHOICE);
    },
};

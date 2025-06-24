export class Session {
    sessionOptions;
    timeoutId;
    constructor(sessionOptions) {
        this.sessionOptions = sessionOptions;
    }
    update() {
        if (typeof this.timeoutId !== 'undefined') {
            clearTimeout(this.timeoutId);
        }
        this.timeoutId = setTimeout(() => {
            this.sessionOptions.callback?.();
        }, this.sessionOptions.duration);
    }
}

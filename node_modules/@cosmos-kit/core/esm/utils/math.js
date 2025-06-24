export function sum(iterable, callbackfn, initialValue) {
    return Array.from(iterable).reduce(callbackfn, initialValue);
}

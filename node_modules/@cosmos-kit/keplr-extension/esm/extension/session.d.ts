export declare class ExpiringLocalStorage {
    /**
     * Add an item to an array in local storage with an expiration time.
     * @param {string} key - The key under which the array is stored.
     * @param {*} value - The value to be added.
     * @param {number} ttl - Time to live in milliseconds.
     */
    static addItem(key: any, value: any, ttl: any): void;
    /**
     * Get all valid (non-expired) items from an array in local storage.
     * @param {string} key - The key of the array to retrieve.
     * @returns {Array} An array of valid items.
     */
    static getItems(key: any): any[];
    /**
     * Remove a specific item from the array in local storage.
     * @param {string} key - The key of the array.
     * @param {*} value - The value to be removed.
     */
    static removeItem(key: any, value: any): void;
}

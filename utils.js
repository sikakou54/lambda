function sleep(msec) {
    return new Promise(resolve => setTimeout(resolve, msec));
}

function getTimeStamp() {
    return (new Date(Date.now() + ((new Date().getTimezoneOffset() + (9 * 60)) * 60 * 1000))).toLocaleString();
}

function getUtcMsec(offset) {
    return (new Date()).getTime() + offset;
}

/**
 * exports
 */
exports.sleep = sleep;
exports.getTimeStamp = getTimeStamp;
exports.getUtcMsec = getUtcMsec;